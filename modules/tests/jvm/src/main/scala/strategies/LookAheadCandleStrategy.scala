package strategies


import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.infixtrading.flashbot.core.DataSource.StreamSelection
import com.infixtrading.flashbot.core.Instrument.CurrencyPair
import com.infixtrading.flashbot.core.MarketData.BaseMarketData
import com.infixtrading.flashbot.core.{MarketData, TimeSeriesMixin, TimeSeriesTap}
import com.infixtrading.flashbot.engine.{SessionLoader, Strategy, TradingSession}
import com.infixtrading.flashbot.models.api.OrderTarget
import com.infixtrading.flashbot.models.core._
import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import org.jquantlib.time.TimeSeries

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

case class Prediction[T](prediction: T, confidence: Double)

trait Predictor1[A1, R] {
  def predict(a1: A1): Prediction[R]
}

trait Predictor2[A1, A2, R] {
  def predict(a1: A1, a2: A2): Prediction[R]
}

trait Predictor3[A1, A2, A3, R] {
  def predict(a1: A1, a2: A2, a3: A3): Prediction[R]
}


case class LookaheadParams(market: Market, sabotage: Boolean)
object LookaheadParams {
  implicit def lookaheadEncoder: Encoder[LookaheadParams] = deriveEncoder
  implicit def lookaheadDecoder: Decoder[LookaheadParams] = deriveDecoder
}

/**
  * A strategy that predicts data one step forwards in time.
  */
class LookAheadCandleStrategy extends Strategy
    with Predictor1[MarketData[Candle], Double]
    with TimeSeriesMixin {

  import FixedSize.dNumeric._

  type Params = LookaheadParams

  override def paramsDecoder = deriveDecoder

  // Source the data from the strategy itself.
  val path1 = DataPath("bitfinex", "eth_usd", "candles_5s")
  def dataSeqs(tr: TimeRange)(implicit mat: Materializer): Map[DataPath, Seq[MarketData[Candle]]] = Map(
    path1 -> Await.result(
      TimeSeriesTap.prices(100.0, .2, .6, tr, 5 seconds).map {
        case (instant, price) =>
          val micros = instant.toEpochMilli * 1000
          BaseMarketData(Candle(micros, price, price, price, price, 0), path1, micros, 1)
      } .toMat(Sink.fold(Seq.empty[MarketData[Candle]]) {
        case (memo, md) => memo :+ md
      })(Keep.right).run(), 15 seconds)
  )

  var staticData = Map.empty[DataPath, Seq[MarketData[Candle]]]

  /**
   * Human readable title for display purposes.
   */
  override def title = "Look Ahead Strategy"

  /**
    * Example strategy that looks ahead by one variable. The strategy must be defined in terms of
    * confidences of the lookahead prediction.
    */
  override def initialize(portfolio: Portfolio, loader: SessionLoader) = {
    import loader._
    Future {
//      staticData = dataSeqs
      Seq("bitfinex/eth_usd/candles_5s")
    }
  }

  var prediction: Option[Double] = None

  override def handleData(md: MarketData[_])(implicit ctx: TradingSession) = md.data match {
    case candle: Candle =>

      record(md.source, md.topic, candle)

      if (prediction.isDefined) {
        if (prediction.get != candle.close) {
          println(s"ERROR: Expected prediction ${prediction.get}, got ${candle}")
        } else {
          println(s"Successful prediction. We predicted ${prediction.get}, and we got $candle")
        }
      }
      // If high confidence prediction, follow it blindly. If low, do nothing.
      val p = predict(md.asInstanceOf[MarketData[Candle]])
      if (p.confidence > .75) {
        prediction = Some(p.prediction)
        val sym = md.path
        val market = Market(md.source, md.topic)
        val pair = CurrencyPair(md.topic)

        // Price about to go up, as much as we can.
        if (prediction.get > candle.close) {
          val account = Account(md.source, pair.quote)
//          println(candle, prediction)
//          println("buy")
//          println(market)
//          println(ctx.getPortfolio)
//          println(ctx.getPortfolio.balance(account))
//          println(ctx.getPortfolio.balance(account).size)
          marketOrder(market, ctx.getPortfolio.balance(account).size)
//          marketOrder(market,
//            FixedSize(ctx.getPortfolio.assets(Account(md.source, pair.quote)), pair.quote))
        } else if (prediction.get < candle.close) {
          val account = Account(md.source, pair.base)
//          println(candle, prediction)
//          println("sell")
//          println(market)
//          println(ctx.getPortfolio)
//          println(ctx.getPortfolio.balance(account))
//          println(-ctx.getPortfolio.balance(account).size)
          // Price about to go down, sell everything!
          marketOrder(market, -ctx.getPortfolio.balance(account).size)
//          marketOrder(market,
//            FixedSize(-ctx.getPortfolio.assets(Account(md.source, pair.base)), pair.base))
        }
      } else {
        prediction = None
      }
  }

  /**
    * Since our data is static, we can cheat on prediction, making it possible to test expected
    * results on random data.
    */
  override def predict(md: MarketData[Candle]): Prediction[Double] = {
    staticData(md.path)
      .dropWhile(_.micros <= md.micros)
      .headOption.map(_.data.close)
      .map(Prediction(_, 1.0)).getOrElse(Prediction(0, 0))
  }

  /**
    * Make the resolved market data lag by one item. This way we can lookahead to the next
    * item being streamed and base our test strategy off of it.
    */
  override def resolveMarketData(streamSelection: StreamSelection)
                                (implicit mat: Materializer)
  : Future[Option[Source[MarketData[_], NotUsed]]] = {

    // Build static data if not yet built.
    if (staticData.isEmpty) {
      staticData = dataSeqs(streamSelection.timeRange)
    }

    // Return it.
    Future.successful(Some(Source(staticData(streamSelection.path).toIndexedSeq)))
  }
}