package flashbot.core

import java.util

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, Cancellable, Scheduler}
import akka.event.LoggingAdapter
import akka.stream.{KillSwitches, Materializer, OverflowStrategy, SharedKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import flashbot.core.Report.ReportError
import flashbot.core.{BuiltInOrder}
import flashbot.core.ReportEvent.{SessionFailure, SessionSuccess}
import flashbot.core.TradingSession.{DataStream, SessionSetup}
import flashbot.models._
import flashbot.server.{ServerMetrics, Simulator}
import flashbot.util._
import flashbot.util.time.FlashbotTimeout
import io.circe.Json
import io.circe.syntax._
import akka.pattern.ask
import flashbot.core.Instrument.CurrencyPair

import scala.annotation.tailrec
import scala.concurrent._
import scala.util.{Failure, Success, Try}

class TradingSession(val strategyKey: String,
                     val params: Json,
                     val mode: TradingSessionMode,
                     val dataServer: ActorRef,
                     val loader: EngineLoader,
                     val log: LoggingAdapter,
                     val initialReport: Report,
                     private val akkaScheduler: Scheduler,
                     private val reportEventsRef: ActorRef,
                     private val portfolioRef: PortfolioRef,
                     private val dataOverrides: Seq[DataOverride[_]],
                     implicit private val mat: Materializer,
                     implicit private val ec: ExecutionContext) {

  protected[flashbot] var id: Option[String] = None

  // Serialize a Json snapshot of the initial report.
//  private val initialReportJson = initialReport.asJson

  // Immediately deserialize to make a deep copy of the report. This is the in-memory
  // report that this session will be using as state.

  protected[flashbot] val prices: PriceIndex = new JPriceIndex(GraphConversions)
  private lazy val sessionSetup = Await.result(load, FlashbotTimeout.default.duration)
  lazy val instruments: InstrumentIndex = sessionSetup.instruments
  lazy val exchanges: Map[String, Exchange] = sessionSetup.exchanges
  lazy val fiatRates: Map[CurrencyPair,Double] = sessionSetup.fiatRates
  private lazy val dataStreams: Seq[Source[MarketData[_], NotUsed]] = sessionSetup.streams

  // Only backtests have an event loop.
  private val eventLoop: Option[EventLoop] =
    if (mode.isBacktest) Some(new EventLoop)
    else None

  protected[flashbot] var scope: OrderRef = _

  private val topLevelOrders = new OrderIndex
  private def currentOrderIndex: OrderIndex = if (scope == null) topLevelOrders else scope.children

  // TODO migrate all of this to scala Maps to prevent getting NULL exceptions
  //val allOrdersByClientId = new java.util.HashMap[String, OrderRef]
  //val allOrdersByExchangeId = new java.util.HashMap[String, OrderRef]

  //val exchangeIdToClientId = new util.HashMap[String, String]()

  def submit(tag: String, order: OrderRef): OrderRef = {
    if (order.ctx != null) {
      throw new RuntimeException("Order already submitted.")
    }

    // Link the session and parent. Generate key.
    order._tag = tag
    order.ctx = this
    order.parent = this.scope
    currentOrderIndex.insert(order)

    // Invoke the submit logic of the order
    order.handleSubmit()

    order
  }

  // Initialize an order and invoke it's submit method.
  def submit(order: OrderRef): OrderRef = {
    submit("", order)
  }

  def cancel(order: OrderRef): Unit = {
    order.handleCancel()
  }

  // Lookup orders in the current scope by tag and cancel them.
  // Also accepts order ids.
  def cancel(id: String): Unit = {

    // Cancel by id
    val byClientId = currentOrderIndex.byClientId.get(id)
    if (byClientId != null) {
      cancel(byClientId)
      return
    }

    // Cancel by key
    val byKey = currentOrderIndex.byKey.get(id)
    if (byKey != null) {
      cancel(byKey)
      return
    }

    // Cancel by tag
    val byTag = currentOrderIndex.byTag.get(id)
    if (byTag != null) {
      var orders: List[OrderRef] = List.empty[OrderRef]
      byTag.forEach((k: String, v: OrderRef) => {
        orders = v :: orders
      })
      cancelOrderList(orders)
      return
    }
  }

  @tailrec
  private def cancelOrderList(orders: List[OrderRef]): Unit = orders match {
    case Nil =>
    case o :: rest =>
      cancel(o)
      cancelOrderList(rest)
  }


  private var killSwitch: SyncVar[Option[SharedKillSwitch]] = new SyncVar()
  //killSwitch.put(None)

  var asyncTickSrc: Option[Source[Tick, NotUsed]] = None
  private val scheduler: EventScheduler =
    if (mode.isBacktest) new EventLoopScheduler
    else {
      val (ref, src) = Source.actorRef[Tick](Int.MaxValue, OverflowStrategy.fail).preMaterialize()
      asyncTickSrc = Some(src)
      new RealTimeScheduler(akkaScheduler, ref)
    }

  def emit(tick: Tick): Unit = scheduler.emit(tick)
  def setTimeout(delayMicros: Long, tick: Tick): Cancellable = scheduler.setTimeout(delayMicros, tick)
  def setTimeout(delayMicros: Long, fn: Runnable): Cancellable = scheduler.setTimeout(delayMicros, fn)
  def setInterval(delayMicros: Long, tick: Tick): Cancellable = scheduler.setInterval(delayMicros, tick)
  def setInterval(delayMicros: Long, fn: Runnable): Cancellable = scheduler.setInterval(delayMicros, fn)

  def emitReportEvent(event: ReportEvent): Unit = {
    reportEventsRef ! event
  }

  def ping: Future[Pong] = {
    val pongPromise = Promise[Pong]
    emit(Callback(() => {
      pongPromise.success(Pong(Long.unbox(seqNr)))
    }))
    pongPromise.future
  }

  protected[flashbot] val exchangeParams: java.util.HashMap[String, ExchangeParams] = buildExMap(_.params)

  // This must be a boxed Long. Intended to be used as a reference to the current tick
  // iteration in weak maps.
  protected[flashbot] var seqNr: java.lang.Long = -1L

  private var strategy: Strategy[_] = _

  private def processTick(tick: Tick): Unit = {
    seqNr = seqNr + 1
    tick match {
      case md: MarketData[_] =>
        //TODO: double check, I'd say this needs to be a call to aroundOnData() instead of onData(), otherwise portfolio will not be initialized
        strategy.aroundOnData(md)(this)

      case callback: Callback =>
        callback.fn.run()

      // TODO: emit also to Report ?
      case req: SimulatedRequest =>
        req.exchange.simulateReceiveRequest(scheduler.currentMicros, req)

      // TODO: emit also to Report to maintain a list of placed/received/canceled orders which should be recorded as events in a time-series
      case event: OrderEvent =>
        val order = event match {
          case r: OrderReceived => findOrder(r.clientOid)
          case o => findOrder(o.orderId)
        }

        strategy.onEvent(event)
        order.map(_._handleEvent(event))

    }
  }

  def findOrder(id: String): Option[OrderRef] = {
    Some(currentOrderIndex.byClientId.get(id))
      .filterNot(_ == null)
      .orElse(Some(currentOrderIndex.byKey.get(id)))
      .filterNot(_ == null)
  }

  private var completeFut: Option[Future[Done]] = None
  def future: Future[Done] = this.synchronized {
    if (completeFut.isEmpty)
      throw new RuntimeException("Session has not started yet.")
    completeFut.get
  }

  /**
    * The main method. It will never run more than once per session instance.
    */
  protected[flashbot] def start(): Future[SessionSetup] = this.synchronized {
    assert(completeFut.isEmpty, "Session already started")

    log.info(s"TradingSession start ${this}")
    val theId = java.util.UUID.randomUUID().toString
    val ks = KillSwitches.shared(theId)

    val sessionInitFuture = for {
      // Load all setup vals
      setup <- load.map(_.copy(sessionId=theId))
      _ = {
        strategy = setup.strategy
        id = Some(setup.sessionId)

        // initialize prices PriceIndex for fiat conversion with fiat instrument rates
        // needed for portfolio conversions for configured targetAssets
        setup.fiatRates.map{el =>
          prices.setPrice(Market("fiat", el._1.symbol), el._2)(setup.instruments)
        }
      }

      // TODO: run fitOn if strategy is instance of TrainableStrategy
      // streams should be backtest - offset (todo: configure offset in strategy?)

      // Prepare market data streams
      (dataStreamsDone, marketDataStream) =

      // If this trading session is a backtest then we merge the data streams by time.
      // But if this is a live trading session then data is sent first come, first serve
      // to keep latencies low.
      dataStreams.reduce[DataStream](
        if (mode.isBacktest) _.mergeSorted(_)(MarketData.orderByTimeAndSeqIdAny) // ordering by time AND seq_id
        else _.merge(_))
//        .alsoTo(Sink.foreach { x =>
//          //log.debug(s"Session DataStream item: $x")
//          println(s"Session DataStream item: $x")
//        })
//        .via(ks.flow)
        .watchTermination()(Keep.right)
        .preMaterialize()

      // Shutdown the session when market data streams are complete.
      _ = dataStreamsDone.onComplete(_ => shutdown()) // was: ks.shutdown() but probably should be session.shutdown()

      // Merge the async tick stream into the main data stream for live data. This will
      // only occur for live and paper trading sessions.
      tickStream =
        if (asyncTickSrc.isDefined)
          marketDataStream.merge(asyncTickSrc.get)
        else marketDataStream

      // Set the kill switch
      _ = {
        killSwitch.put(Some(ks))
      }
    } yield {
      log.debug(s"Session init completed $setup")
      (setup, tickStream)
    }




    /**
      * =============
      *   Lift-off
      * =============
      */
    val mainLoopFut = for {
      (setup,tickStream) <- sessionInitFuture
      done <- tickStream runForeach { tick =>
        tick match {
          case md: MarketData[_] =>

            // If this data has price info attached, save it to the price index. Also update
            // the portfolio in case there are any positions that need to be initialized.
            md.data match {
              case pd: Priced =>
                prices.setPrice(Market(md.source, md.topic), pd.price)(instruments)
                portfolioRef.update(this, _.initializePositions(prices, instruments, ServerMetrics))

              case _ => // ignore
            }

            // Update the simulator with the new market data. This lets it emit fills and stuff.
            if (mode.isBacktest && setup.exchanges.isDefinedAt(md.source)) {
              val sim = setup.exchanges(md.source).asInstanceOf[Simulator]
              sim.marketDataUpdate(md)
            }

            // Fast forward the event loop.
            println(s"${md.micros} \t $md")
            scheduler.fastForward(md.micros, processTick)

          case _ =>
        }

        // Then process the tick itself
        processTick(tick)

      } andThen {
        // After the market data streams are done, fast forward the event loop so that backtests
        // can process all ticks scheduled for after the last piece of market data.
        case Success(_) =>
          scheduler.fastForward(Long.MaxValue, processTick)
        case other =>
          log.debug(s"mainLoop result: $other")
      }
    } yield done

    // Emit the SessionComplete event
    completeFut = Some(mainLoopFut andThen {
      case Success(Done) =>
        emitReportEvent(SessionSuccess)
      case Failure(err) =>
        log.error(err, "TradingSession error")
        emitReportEvent(SessionFailure(ReportError(err)))
    })

    sessionInitFuture.map(_._1)
  }

  def buildExMap[T](fn: Exchange => T): java.util.HashMap[String, T] = {
    val map = new java.util.HashMap[String, T]()
    exchanges.foreach { ex =>
      map.put(ex._1, fn(ex._2))
    }
    map
  }

  def shutdown(): Future[Done] = for {
    ks <- Future(blocking(killSwitch.take))
    _ <-
      if (ks.isEmpty) Future.failed(
        new RuntimeException("TradingSession has not been started."))
      else {
        ks.get.shutdown()
        Future.successful(Done)
      }
    _ = { killSwitch.put(ks) }
    _ = {
      log.info(s"TradingSession successfully shutdown")
    }
  } yield Done

  def getPortfolio: Portfolio = portfolioRef.getPortfolio(Some(instruments))

  protected[flashbot] lazy val load: Future[SessionSetup] = {

    val exchangeConfigs = loader.getExchangeConfigs()

    log.debug("Exchange configs: {}", exchangeConfigs)

    // Set the time. Using system time just this once.
    val sessionStartMicros = System.currentTimeMillis() * 1000
    def dataSelection[T](path: DataPath[T]): DataSelection[T] = mode match {
      case Backtest(range) =>
        DataSelection(path, Some(range.start), Some(range.end))
      case liveOrPaper =>
        DataSelection(path, Some(sessionStartMicros - liveOrPaper.lookback.toMicros), None)
    }

    // Load a new instance of an exchange.
    def loadExchange(name: String): Try[Exchange] =
      loader.loadNewExchange(name)
        .map(plainInstance => {
          // Wrap it in our Simulator if necessary.
          if (mode == Live) plainInstance
          else new Simulator(plainInstance, this)
        })

    log.debug("Starting async setup")

    def loadStrat[T <: StrategyParams](clazz: String): Future[Strategy[T]] = for {
      // Load the strategy
      strategy <- Future.fromTry[Strategy[T]](loader.loadNewStrategy[T](clazz))

      _ <- for {
        // Decode the params
        decodedParams <- strategy.decodeParams(params.noSpaces).toFut

        // Setup the context
        _ = strategy.setCtx(this)

        // Load params into strategy
        _ = strategy.setParams(decodedParams)

        // Set the var buffer
        _ = strategy.setVarBuffer(new VarBuffer(debox.Map.fromIterable(initialReport.getValues).mapValues(_.value)))

        // Set the bar size
        _ = strategy.setSessionBarSize(initialReport.barSize)
      } yield strategy
    } yield strategy

    for {


      // Check that we have a config for the requested strategy.
      strategyClassName <- loader.strategyClassNames
        .get(strategyKey)
        .toTry(s"Unknown strategy: $strategyKey")
        .toFut

      // Load the instruments
      instruments <- loader.loadInstruments

      // Load strategy
      strategy <- {
        val strat: Future[Strategy[_ <: StrategyParams]] = loadStrat(strategyClassName)
        strat
      }

      // Initialize the strategy and collect data paths
      paths <- strategy.initialize(portfolioRef.getPortfolio(Some(instruments)), loader)

      // Load the exchanges PLUS special 'fiat' exchange
      // TODO: better to add a "isFiat" to Instrument ?
      exchangeNames: Set[String] = Some(strategy.requiresFiatRates).filter(_ == true).map(_ => Set("fiat")).getOrElse(Set.empty) ++
        paths.toSet[DataPath[_]].map(_.source)
          .intersect(exchangeConfigs.keySet)
      _ = { log.debug("Loading exchanges: {}.", exchangeNames) }

      exchanges: Map[String, Exchange] <- Future.sequence(exchangeNames.map(n =>
        loadExchange(n).map(n -> _).toFut)).map(_.toMap)
      _ = { log.debug("Loaded exchanges: {}.", exchanges) }

      // Resolve market data streams.
      streams <- Future.sequence(paths.map(path =>
        strategy.resolveMarketData(dataSelection(path), dataServer, dataOverrides)))

      _ = { log.debug("Resolved {} market data streams out of" +
        " {} paths requested by the strategy.", streams.size, paths.size) }

      // fetch current fiat rates from exchange associated by "fiat" key
      fiatRates <- exchanges.get("fiat") match {
        case Some(fEx) if fEx.isInstanceOf[Simulator] && fEx.asInstanceOf[Simulator].base.isInstanceOf[HasFiatRates] =>
          fEx.asInstanceOf[Simulator].base.asInstanceOf[HasFiatRates].fiatRates(instruments.byExchange("fiat").map(_.symbol))
        case Some(fEx) if fEx.isInstanceOf[HasFiatRates] =>
          fEx.asInstanceOf[HasFiatRates].fiatRates(instruments.byExchange("fiat").map(_.symbol))
        case other =>
          log.warning(s"No FIAT exchange configured to fetch fiat rates! {}", other)
          Future.successful(Map.empty[CurrencyPair,Double])
      }

    } yield SessionSetup(instruments, exchanges, strategy, java.util.UUID.randomUUID().toString,
      streams, fiatRates, sessionStartMicros)
  }
}

object TradingSession {
  type DataStream = Source[MarketData[_], NotUsed]
  type TickStream = Source[Tick, NotUsed]

  case class SessionSetup(instruments: InstrumentIndex,
                          exchanges: Map[String, Exchange],
                          strategy: Strategy[_ <: StrategyParams],
                          sessionId: String,
                          streams: Seq[Source[MarketData[_], NotUsed]],
                          fiatRates: Map[CurrencyPair,Double],
                          sessionMicros: Long)
}




//trait TradingSession {
//  def id: String
////  def send(event: Any): Unit
////  def send(events: mutable.Buffer[Any])
//  def getPortfolio: Portfolio
////  def cmdQueues: java.util.Map[String, CommandQueue]
//  def prices: PriceIndex
//  def instruments: InstrumentIndex
//  protected[flashbot] def exchanges: Map[String, Exchange]
//  def exchangeParams: java.util.Map[String, ExchangeParams]
////  protected[flashbot] def orderManagers: java.util.Map[String, TargetManager]
//
//  // A weak reference to this iteration of the session. It must be GC'd after the current
//  // iteration is done. Therefore, DO NOT HOLD ON TO THIS REFERENCE! Instead, use it as
//  // the key in WeakHashMaps for caching computations in between Strategy mixins.
//  protected[flashbot] def ref: java.lang.Long
//
//  def tryRound(market: Market, size: FixedSize): Option[FixedSize]
//  def round(market: Market, size: FixedSize): FixedSize =
//    tryRound(market, size).getOrElse({
//      throw new RuntimeException(s"Can't round $size for market $market")
//    })
//}

//trait TickCollector {
//  def insertTick(event: ): Unit
//}
//
//class BacktestTickCollector extends TickCollector {
//  override def insert(event: Any) = ???
//}

/**
  * Live tick collector simply sends each tick to the tick actor ref.
  */
//class LiveTickCollector(tickRef: ActorRef) extends TickCollector {
//  override def insert(tick: Tick) = tickRef ! tick
//}


