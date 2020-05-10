package flashbot.exchanges
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import flashbot.core.Instrument.CurrencyPair
import flashbot.core.{Exchange, ExchangeParams, Instrument, InstrumentParams}
import flashbot.models.PostOrderRequest

import scala.concurrent.Future

class Coinbase(implicit val system: ActorSystem,
               val mat: Materializer) extends Exchange {

//  override def takerFee = 0.003
//  override def takerFee = -0.00035

  // TODO: pull /products from CoinbasePro API and initialize params !!
  override def instruments: Future[Set[Instrument]] = Future.successful(Set.empty)

  override val params: ExchangeParams = {
    new ExchangeParams(
      // Base params
      new InstrumentParams(){{
        makerFee = .5 * (1/100)
        takerFee = .5 * (1/100)
      }},

      // TODO: add min. trade amount !

      // Instrument specific
      new java.util.HashMap[String, InstrumentParams](){{
        put("btc_usd", new InstrumentParams(){{
          tickSize = 0.01
          minOrderSize = 0.001 // BTC
        }})
        put("eth_usd", new InstrumentParams(){{
          tickSize = 0.01
          minOrderSize = 0.1 // ETH
        }})
        put("btc_eur", new InstrumentParams(){{
          tickSize = 0.01
          minOrderSize = 25 //min. 25 EUR
        }})
        put("eth_eur", new InstrumentParams(){{
          tickSize = 0.01
          minOrderSize = 25
        }})
        put("eth_btc", new InstrumentParams(){{
          tickSize = 0.01
          minOrderSize = 0.001
        }})
        put("ltc_eur", new InstrumentParams(){{
          tickSize = 0.01
          minOrderSize = 0.1 // LTC
        }})
      }})
  }

  override def order(req: PostOrderRequest) = ???
  override def cancel(id: String, pair: Instrument) = ???


  override def baseAssetPrecision(pair: Instrument): Int = pair match {
    case CurrencyPair("eur", "usd") => 5
    case _ => 8
  }

  override def quoteAssetPrecision(pair: Instrument): Int = pair match {
    case CurrencyPair("xrp", "eur") => 5
    case CurrencyPair("xrp", "usd") => 5
    case CurrencyPair("eur", "usd") => 5
    case CurrencyPair(_, "usd") => 2
    case CurrencyPair(_, "eur") => 2
    case _ => 8
  }

  override def fetchPortfolio = Future.successful((Map.empty, Map.empty))
}
