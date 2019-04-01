package flashbot.core

import flashbot.models.Market

final case class TargetId(market: Market, key: String) {
  def instrument(implicit idx: InstrumentIndex) = idx(market)

  override def toString = s"$market:$key"
}

