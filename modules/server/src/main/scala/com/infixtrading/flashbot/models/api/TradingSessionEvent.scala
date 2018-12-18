package com.infixtrading.flashbot.models.api
import com.infixtrading.flashbot.models.core.FixedSize.FixedSizeD
import com.infixtrading.flashbot.models.core.{FixedSize, Market}
import com.infixtrading.flashbot.report.ReportEvent

trait TradingSessionEvent
case class LogMessage(message: String) extends TradingSessionEvent
case class OrderTarget(market: Market,
                       key: String,
                       size: FixedSize[Double],
                       price: Option[Double],
                       once: Option[Boolean] = None,
                       postOnly: Option[Boolean] = None) extends TradingSessionEvent {
  def id: String = s"$market:$key"
}

