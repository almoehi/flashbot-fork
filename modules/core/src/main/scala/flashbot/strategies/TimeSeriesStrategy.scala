package flashbot.strategies

import flashbot.core.{MarketData, Strategy, _}
import flashbot.models.{DataPath, Market, Portfolio}
import io.circe.generic.JsonCodec
import io.circe.parser._
import TimeSeriesStrategy._
import flashbot.server.ServerMetrics

import scala.concurrent.Future

class TimeSeriesStrategy extends Strategy[Params] with TimeSeriesMixin {

  override def title = "Time Series Strategy"

  override def decodeParams(paramsStr: String) = decode[Params](paramsStr).toTry

  override def initialize(portfolio: Portfolio, loader: EngineLoader): Future[Seq[DataPath[_]]] =
    Future.successful(Seq(params.path))

  override def onData(marketData: MarketData[_]): Unit = marketData.data match {

    case trade: Trade =>
      recordTimeSeries(s"price", marketData.micros, trade.price)
      recordTimeSeries(s"volume", marketData.micros, trade.size)
      recordTrade(Market(marketData.source, marketData.topic), marketData.micros, trade.price, Some(trade.size))

    case pd: Priced =>
      ServerMetrics.inc("time_series_strategy_data_count")
      recordTimeSeries(s"price", marketData.micros, pd.price)

    case x => // Ignore non-priced data
  }
}

object TimeSeriesStrategy {
  @JsonCodec case class Params(path: String)
}
