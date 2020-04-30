package flashbot.exchanges

import akka.actor.ActorSystem
import akka.stream.Materializer
import flashbot.core.Instrument.{FuturesContract, Index}
import flashbot.core._
import flashbot.models.{ExchangeResponse, PostOrderRequest}

import scala.concurrent.Future

class BitMEX(implicit val system: ActorSystem,
             val mat: Materializer) extends Exchange {

  override val params: ExchangeParams = {
    new ExchangeParams(
      // Base params
      new InstrumentParams(){{
      }},

      // Instrument specific
      new java.util.HashMap[String, InstrumentParams](){{
        put("xbtusd", new InstrumentParams(){{
          makerFee = -0.00025d
          takerFee = 0.00075d
          tickSize = 0.01
        }})
        put("ethusd", new InstrumentParams(){{
          makerFee = -0.00025d
          takerFee = 0.00075d
          tickSize = 0.05
        }})
      }})
  }

  override def cancel(id: String, pair: Instrument): Future[ExchangeResponse] = ???

  override def order(req: PostOrderRequest): Future[ExchangeResponse] = ???

  override def baseAssetPrecision(pair: Instrument): Int = ???

  override def quoteAssetPrecision(pair: Instrument): Int = ???

  override def lotSize(pair: Instrument): Option[Double] = ???

  override def instruments =
    Future.successful(Set(BitMEX.XBTUSD, BitMEX.ETHUSD))

  override def fetchPortfolio = Future.successful((Map.empty, Map.empty))
}

object BitMEX {

  object XBTUSD extends FuturesContract {
    override def symbol = "xbtusd"
    override def base = "xbt"
    override def quote = "usd"
    override def settledIn = Some("xbt")

//    override def markPrice(prices: PriceIndex) = 1.0 / prices(symbol)

    override def security = Some(symbol)

    // https://www.bitmex.com/app/seriesGuide/XBT#How-is-the-XBTUSD-Perpetual-Contract-Quoted
    override def pnl(size: Double, entryPrice: Double, exitPrice: Double) = {
      size * (1.0d / entryPrice - 1.0d / exitPrice)
    }

    override def valueDouble(price: Double): Double = 1d / price

    //override def tickSize: Double = 0.05
  }

  object ETHUSD extends FuturesContract {
    override def symbol = "ethusd"
    override def base = "eth"
    override def quote = "usd"
    override def settledIn = Some("xbt")

    val bitcoinMultiplier: Double = 0.000001

//    override def markPrice(prices: PriceIndex) = ???

    override def security = Some(symbol)

    // https://www.bitmex.com/app/seriesGuide/ETH#How-Is-The-ETHUSD-Perpetual-Contract-Quoted
    override def pnl(size: Double, entryPrice: Double, exitPrice: Double) = {
      (exitPrice - entryPrice) * bitcoinMultiplier * size
    }

    override def valueDouble(price: Double): Double = price * bitcoinMultiplier

    //override def tickSize: Double = 0.05
  }

  object BXBT extends Index(".BXBT", "xbt", "usd")
}
