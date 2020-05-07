package flashbot.core

import flashbot.core.Report._
import flashbot.models._
import io.circe._
import io.circe.generic.JsonCodec

import scala.collection.mutable

@JsonCodec
sealed trait PortfolioDelta extends ReportEvent

@JsonCodec
case class BalanceUpdated(account: Account, balance: Option[Double]) extends PortfolioDelta

@JsonCodec
case class DefaultTargetAssetUpdated(asset: String) extends PortfolioDelta

@JsonCodec
case class PositionUpdated(market: Market, position: Option[Position]) extends PortfolioDelta

@JsonCodec
case class OrdersUpdated(market: Market, bookDelta: OrderBook.Delta) extends PortfolioDelta

@JsonCodec
case class BatchPortfolioUpdate(deltas: mutable.Buffer[PortfolioDelta]) extends PortfolioDelta


/**
  * These are events that are emitted by the session, to be sent to the report.
  */
@JsonCodec
sealed trait ReportEvent

object ReportEvent {

  @JsonCodec
  case class TradeEvent(id: Option[String],
                        exchange: String,
                        product: String,
                        micros: Long,
                        price: Option[Double],
                        size: Double) extends ReportEvent with Timestamped

  @JsonCodec
  case class PriceEvent(market: Market,
                        price: Double,
                        micros: Long) extends ReportEvent with Timestamped

//  case class PositionEvent(market: Market,
//                           position: Position,
//                           micros: Long) extends ReportEvent with Timestamped

//  case class BalanceEvent(account: Account,
//                          balance: Double,
//                          micros: Long) extends ReportEvent with Timestamped

  @JsonCodec
  sealed trait CandleEvent extends ReportEvent {
    def series: String
  }
  case class CandleUpdate(series: String, candle: Candle) extends CandleEvent
  case class CandleAdd(series: String, candle: Candle) extends CandleEvent

  @JsonCodec
  case class CollectionEvent(name: String, item: Json) extends ReportEvent

  @JsonCodec
  sealed trait SessionComplete extends ReportEvent
  case object SessionSuccess extends SessionComplete
  case class SessionFailure(err: ReportError) extends SessionComplete

  @JsonCodec
  sealed trait ValueEvent extends ReportEvent
  case class PutValueEvent(key: String, fmtName: String, value: Json) extends ValueEvent
  case class UpdateValueEvent(key: String, delta: Json) extends ValueEvent
  case class RemoveValueEvent(key: String) extends ValueEvent

}
