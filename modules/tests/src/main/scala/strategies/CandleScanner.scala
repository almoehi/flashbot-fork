package strategies
import akka.NotUsed
import akka.actor.ActorRef
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import flashbot.core.MarketData.BaseMarketData
import flashbot.core.{EngineLoader, _}
import flashbot.models.{Candle, DataOverride, DataSelection, Portfolio}
import io.circe.generic.JsonCodec
import io.circe.generic.auto._
import io.circe.parser._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

@JsonCodec
case class CandleScannerParams(reportTargetAsset: String = "usd") extends StrategyParams

class CandleScanner extends Strategy[CandleScannerParams] {

  def decodeParams(paramsStr: String) = decode[CandleScannerParams](paramsStr).toTry

  override def title = "Candle Scanner"

  override def initialize(portfolio: Portfolio, loader: EngineLoader) = {
    Future.successful(Seq("coinbase/eth_eur/candles_1d"))
  }

  override def onData(data: MarketData[_]) = {
    println(data)
  }

  override def resolveMarketData[T](selection: DataSelection[T], dataServer: ActorRef,
                                    dataOverrides: Seq[DataOverride[Any]])
                                   (implicit mat: Materializer, ec: ExecutionContext)
      : Future[Source[MarketData[T], NotUsed]] = {
    Future.successful(PriceTap
      .akkaStream(1 day)
      .via(PriceTap.aggregatePricesFlow(1 day))
      .throttle(1, 200 millis)
      .zipWithIndex
      .map {
        case (candle, i) => BaseMarketData(candle.asInstanceOf[T], selection.path, candle.micros, 1, i.toLong)
      }
    )
  }
}
