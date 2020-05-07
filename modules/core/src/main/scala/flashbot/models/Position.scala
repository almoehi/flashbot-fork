package flashbot.models

import flashbot.core.Instrument
import flashbot.core.Instrument.Derivative
import io.circe.{Decoder, Encoder}

/**
  * Positions are used to calculate the portfolio equity and PnL.
  *
  * @param size positive (long) or negative (short) position in some security.
  * @param leverage the leverage of the position. 1 for no leverage used.
  * @param entryPrice this will be NaN for uninitialized positions, which are positions
  *                   that may be used for the initial portfolio in backtests, where we
  *                   don't know the entry price at the time of portfolio creation.
  */
case class Position(size: Double, leverage: Double, entryPrice: Double) {

  /**
    * Updates the position size and average entry price.
    * Returns the new position and any realized PNL that occurred as a side effect.
    */
  def updateSize(newSize: Double, instrument: Instrument, price: Double): (Position, Double) = {

    // First stage, close existing positions if necessary. Record PNL.
    var pnl = 0d
    var tmpSize = size
    if (isShort && newSize > size) {
      tmpSize = newSize min 0
    } else if (isLong && newSize < size) {
      tmpSize = newSize max 0
    }

    if (tmpSize != size) {
      pnl = instrument.pnl(size - tmpSize, entryPrice, price)
    }

    // Second stage, increase to new size and update entryPrice.
    val enterSize = (newSize - tmpSize).abs
    val newEntryPrice = (tmpSize * entryPrice + enterSize * price) / (tmpSize + enterSize)

    (this.copy(size=newSize,entryPrice = newEntryPrice), pnl)
  }

  def withEntryPrice(price: Double) = this.copy(entryPrice = price)
  def withSize(sz: Double) = this.copy(size = sz)
  def withLeverage(l: Double) = this.copy(leverage = l)

  def isLong: Boolean = size > 0
  def isShort: Boolean = size < 0

  def initialMargin(instrument: Instrument): Double = instrument match {
    case instr:Derivative =>
      instr.valueDouble(entryPrice) * size.abs / leverage
    case instr:Instrument => // leverge == 1.0
      assert(leverage == 1.0, s"leverage for non-derivative instrument ($instr) should be 1")
      instr.valueDouble(entryPrice) * size.abs / leverage
    case _ => 0d
  }


  def isInitialized: Boolean = java.lang.Double.isNaN(entryPrice)

  override def toString: String = Seq(
    size.toString,
    if (leverage == 1) "" else "x" + leverage,
    "@" + entryPrice
  ).mkString("")
}

object Position {

  def parse(str: String): Position = {
    str.split("@").toList match {
      case snl :: entryPrice :: Nil =>
        snl.split("x").toList match {
          case size :: Nil =>
            new Position(size.toDouble, 1, entryPrice.toDouble)
          case size :: leverage :: Nil =>
            new Position(size.toDouble, leverage.toDouble, entryPrice.toDouble)
        }
    }
  }

  implicit val positionEncoder: Encoder[Position] = Encoder[String].contramap(_.toString)
  implicit val positionDecoder: Decoder[Position] = Decoder[String].map(parse)
}
