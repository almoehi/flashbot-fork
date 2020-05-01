package flashbot.core

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.Materializer
import akka.pattern.ask
import flashbot.core.FlashbotConfig.ExchangeConfig
import flashbot.core.Instrument.CurrencyPair
import flashbot.models.{DataPath, Market, MarketDataIndexQuery}
import flashbot.util.time.FlashbotTimeout

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Loads miscellaneous data required throughout the system. One EngineLoader
  * instance exists per TradingEngine instance.
  */
class EngineLoader(val getExchangeConfigs: () => Map[String, ExchangeConfig],
                   dataServer: ActorRef,
                   protected[flashbot] val strategyClassNames: Map[String, String])
                  (implicit system: ActorSystem, mat: Materializer) {
  implicit val timeout = FlashbotTimeout.default
  implicit val ec: ExecutionContext = system.dispatcher

  def exchanges: Set[String] = getExchangeConfigs().keySet

  def loadInstruments: Future[InstrumentIndex] = Future.sequence(
    getExchangeConfigs().map {
      case (key, config) =>
        loadNewExchange(key).get.instruments.map(_ ++
            config.pairs.getOrElse(Seq.empty).map(CurrencyPair(_)).toSet)
        .map(key -> _)
    }).map(i => new InstrumentIndex(i.toMap))

  def markets: Future[Set[Market]] = for {
    index: Map[Long, DataPath[Any]] <-
      (dataServer ? MarketDataIndexQuery).mapTo[Map[Long, DataPath[Any]]]
  } yield index.values.map(_.market).toSet

  protected[flashbot] def loadNewExchange(name: String, ctx: TradingSession = null): Try[Exchange] = {

    val config = getExchangeConfigs().get(name)
    if (config.isEmpty) {
      return Failure(new RuntimeException(s"Exchange $name not found"))
    }

    try {
      Success(
        getClass.getClassLoader
          .loadClass(config.get.`class`)
          .asSubclass(classOf[Exchange])
          .getConstructor(classOf[ActorSystem], classOf[Materializer])
          .newInstance(system, mat).withParams(config.get.params))
    } catch {
      case err: ClassNotFoundException =>
        Failure(new RuntimeException("Exchange class not found: " + config.get.`class`, err))
      case err: ClassCastException =>
        Failure(
          new RuntimeException(s"Class ${config.get.`class`} must be a " +
                        s"subclass of flashbot.core.Exchange", err))
    }
  }

  protected[flashbot] def loadNewStrategy[P <: StrategyParams](className: String): Try[Strategy[P]] =
    try {
      val clazz = getClass.getClassLoader.loadClass(className).asSubclass(classOf[Strategy[P]])
      Success(clazz.newInstance())
    } catch {
      case err: ClassNotFoundException =>
        Failure(new RuntimeException(s"Strategy class not found: $className", err))

      case err: ClassCastException =>
        Failure(new RuntimeException(s"Class $className must be a " +
          s"subclass of flashbot.core.Strategy", err))
      case err => Failure(err)
    }

  protected[flashbot] def strategyInfo(className: String): Future[StrategyInfo] = {
    val strategy = loadNewStrategy(className).get
    strategy.info(this).map(_.copy(title = strategy.title))
  }

  protected[flashbot] def allStrategyInfos: Future[Map[String, StrategyInfo]] = {
    val (keys, classNames) = strategyClassNames.toSeq.unzip
    Future.sequence(classNames.map(strategyInfo))
      .map(keys zip _).map(_.toMap)
  }
}
