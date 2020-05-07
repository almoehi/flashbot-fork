package flashbot.exchanges

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import flashbot.core.Instrument.CurrencyPair
import flashbot.core.{Exchange, ExchangeParams, HasFiatRates, Instrument, InstrumentParams, Trade}
import flashbot.models.PostOrderRequest
import com.softwaremill.sttp._
import scala.concurrent.Future
import flashbot.util.network.RequestService._
import io.circe.parser._
import io.circe.generic.JsonCodec

class ExchangeRatesApiIO(implicit val system: ActorSystem,
                         val mat: Materializer) extends Exchange with HasFiatRates {

  import ExchangeRatesApiIO._

  lazy val log = system.log
  implicit val ec = system.dispatcher

  val params = new ExchangeParams(null, new java.util.HashMap[String, InstrumentParams]())

  override def order(req: PostOrderRequest) = ???

  override def cancel(id: String, pair: Instrument) = ???

  override def baseAssetPrecision(pair: Instrument): Int = 5

  override def quoteAssetPrecision(pair: Instrument): Int = 5

  override def fetchPortfolio = Future.successful((Map.empty, Map.empty))

  def fiatRates(pairs: Set[String]): Future[Map[CurrencyPair,Double]] = {

    val p = pairs.map(CurrencyPair.parse(_)).filter(_.isDefined).map(_.get)
    val syms = (p.map(_.base) ++ p.map(_.quote)).toSet

    val futRates = (p ++ p.map(_.flipped)).map{pair =>
      val symList = syms.filterNot(_==pair.base).map(_.toUpperCase).mkString(",")
      val url = uri"https://api.exchangeratesapi.io/latest?base=${pair.base.toUpperCase}&symbols=$symList"

      sttp.get(url).sendWithRetries().map { rsp =>
        rsp.body match {
          case Left(err) =>
            log.error("Error in request to {}: {}\n{}", err, url, rsp.body)
            Left(err)
          case Right(bodyStr) =>
            val mabeResp = decode[ExchangeRatesApiResponse](bodyStr).toTry.toOption
            Right(mabeResp.flatMap(_.getQuotePrice(pair.quote.toUpperCase))
              .map(d => (pair,ForexMessage(id = pair.toString + s"-${Instant.now.toEpochMilli}", symbol = pair.quote, price = d))))
        }
      }
    }

    Future.sequence(futRates).map(_.collect{
      case Right(Some((pair,trade))) => (pair,trade.price)
    }.toMap).recover{
      case t:Throwable =>
        log.error(t, "Failed to fetch fiat rates")
        Map.empty[CurrencyPair,Double]
    }

  }

}


object ExchangeRatesApiIO {

  @JsonCodec
  case class ForexMessage(id: String, millis: Long = Instant.now.toEpochMilli, symbol: String, price: Double) {
    def toTrade = Trade(id, millis * 1000, price, 1, "up") // TODO: determine up/down based on previous values
  }

  @JsonCodec
  case class ExchangeRatesApiResponse(base: String, date: String, rates: Map[String,Double]) {
    def getQuotePrice(q: String) = rates.get(q.toUpperCase)
  }

}