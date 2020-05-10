package flashbot.strategies

import flashbot.core.DataType.{CandlesType, TradesType}
import flashbot.core._
import flashbot.core.FixedSize._
import flashbot.core.AssetKey.implicits._
import flashbot.models.Order.{Buy, Sell}
import flashbot.models.{DataPath, Market, Portfolio}
import io.circe.generic.JsonCodec
import io.circe.parser._
import flashbot.util.time._
import com.github.andyglow.jsonschema.AsCirce._
import scala.concurrent.duration._
import org.ta4j.core.indicators.SMAIndicator
import org.ta4j.core.indicators.helpers.ClosePriceIndicator
import org.ta4j.core.trading.rules.{CrossedDownIndicatorRule, CrossedUpIndicatorRule, StopGainRule, StopLossRule}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try
import scala.language.{implicitConversions, postfixOps}

@JsonCodec
case class DMACParams(market: String, smaShort: Int, smaLong: Int, stopLoss: Double = 0.97d, takeProfit: Double = 1.02d, reportTargetAsset: String = "usd") extends StrategyParams

class DMACStrategy extends Strategy[DMACParams] with TimeSeriesMixin /*with OrderManagement*/ {
  override def decodeParams(paramsStr: String): Try[DMACParams] = decode[DMACParams](paramsStr).toTry

  override def title = "Dual Moving Average Crossover"

  lazy val market = Market(params.market)
  lazy val close = new ClosePriceIndicator(prices(market))
  lazy val smaShort = new SMAIndicator(close, params.smaShort)
  lazy val smaLong = new SMAIndicator(close, params.smaLong)

  lazy val crossedUp = new CrossedUpIndicatorRule(smaShort, smaLong)
  lazy val crossedDown = new CrossedDownIndicatorRule(smaShort, smaLong)

  override def initialize(portfolio: Portfolio, loader: EngineLoader): Future[Seq[DataPath[Nothing]]] =
    Future.successful(Seq(DataPath(market, "candles_1m")))

  override def requiredWarmupDuration = 2 seconds

  var isLong = false
  var enteredAt: Double = -1d


  lazy val stopLoss = params.stopLoss
  lazy val takeProfit = params.takeProfit
  override def defaultTargetAsset = params.reportTargetAsset

  override def onEvent(event: OrderEvent): Unit = {
    // TODO: record ordering events
    //println(s"onEvent: $event")
  }
  override def onData(data: MarketData[_]): Unit = {
    val portfolio = ctx.getPortfolio
    val balance: FixedSize = portfolio.getBalanceSize(market.settlementAccount)
    val holding = portfolio.getBalanceSize(market.securityAccount)
    val price = try {
      getPrice(market)
    } catch {
      case t:Throwable =>
        ctx.log.error(t, s"Unable to retrieve price for: $market")
        return
    }

    //println(s"balance: ${balance.amount} ${market.settlementAccount.security} | holding: ${holding.amount} ${market.securityAccount.security} | portfolio: ${portfolio}")
    println(s"balance: ${balance.amount} ${market.settlementAccount.security} | holding: ${holding.amount} ${market.securityAccount.security}" +
      s" | availableSettled: ${portfolio.getAvailableBalance(market.settlementAccount)} ${market.settlementAccount.security}" +
      s" | availableSecurity: ${portfolio.getAvailableBalance(market.securityAccount)} ${market.securityAccount.security}" +
      s" | orderMargin: ${portfolio.getOrderMargin(market)}" +
      s" | positionMargin: ${portfolio.getPositionMargin(market)}" +
      s" | pnl: ${portfolio.getPositionPnl(market)}" +
      s" | position: ${portfolio.getPosition(market)}"
    )
    val hasCrossedUp = crossedUp.isSatisfied(index(market))
    val hasCrossedDown = crossedDown.isSatisfied(index(market))

    //recordTimeSeries("equity", data.micros, portfolio.getEquity(market.settlementAccount.security))

    recordTimeSeries("position", data.micros,
      portfolio.getBalance(market.securityAccount))

    recordTimeSeries("available_balance_settled", data.micros,
      portfolio.getAvailableBalance(market.settlementAccount))

    recordTimeSeries("available_balance_security", data.micros,
      portfolio.getAvailableBalance(market.securityAccount))

    recordTimeSeries("order_margin", data.micros,
      portfolio.getOrderMargin(market))

    recordTimeSeries("position_margin", data.micros,
      portfolio.getPositionMargin(market))

    recordTimeSeries("pnl", data.micros,
      portfolio.getPositionPnl(market))

    recordTimeSeries("cash", data.micros,
      portfolio.getBalance(market.settlementAccount))

    recordTimeSeries("smaShort",
      smaShort.getTimeSeries.getLastBar.getEndTime.toInstant.micros,
      smaShort.getValue(smaShort.getTimeSeries.getEndIndex).doubleValue())

    recordTimeSeries("smaLong",
      smaLong.getTimeSeries.getLastBar.getEndTime.toInstant.micros,
      smaLong.getValue(smaLong.getTimeSeries.getEndIndex).doubleValue())

    // 3% stop loss
    val stopLossTriggered = isLong && price < enteredAt * stopLoss

    // 2% take profit
    val takeProfitTriggered = isLong && price > enteredAt * takeProfit

    if (hasCrossedUp && !isLong && balance.amount > 0.0) {
      isLong = true
      enteredAt = price
      val amount = balance.as(market.baseAccount).amount * portfolio.getLeverage(market)
      println(s"BUY: ${amount}")
      ctx.submit(new MarketOrder(market, amount))
      recordTimeSeries("entry_exit", data.micros, 1)

    } else if (holding.amount > 0.0 &&  (stopLossTriggered || takeProfitTriggered || (hasCrossedDown && isLong))) {
      isLong = false
      enteredAt = -1
      println(s"SELL: ${holding.amount}")
      ctx.submit(new MarketOrder(market, -holding.amount))
      recordTimeSeries("entry_exit", data.micros, -1)
    }
  }

  override def info(loader: EngineLoader) = for {
    markets <- loader.markets
    initial <- super.info(loader)
  } yield initial
    // Generate a JSON Schema automatically from the params class.
    .withSchema(json.Json.schema[DMACParams].asCirce().noSpaces)
    .withParamKeys(json.Json.schema[DMACParams].asCirce().hcursor.keys.map(_.toSeq).getOrElse(Seq.empty))

    // Add available options to the "market", "datatype", and "fairPriceIndicator" params.
    // This also sets the default for each parameter as the third argument.
    .withParamOptionsOpt("market", markets.toSeq, markets.headOption)
    //.withParamOptions("datatype", Seq(TradesType, CandlesType(1 minute)), TradesType)
    .withParamOptions("reportTargetAsset", Seq("eur","usd"), "eur")

    // Set defaults for the rest of the fields.
    // TODO: can we somehow derive them fomr the DMACStrategyParam case class ???
    .withParamDefault("smaShort", 7)
    .withParamDefault("smaLong", 14)
    .withParamDefault("stopLoss", 0.97d)
    .withParamDefault("takeProfit", 1.05d)
    .withParamDefault("reportTargetAsset", "eur")


    // Update the layout
    .updateLayout(_
    .addPanel("Prices")
    //.addQuery("price", "$market Price", "Prices")
    //.addTimeSeries("price", "Prices")
    .addTimeSeries("${market:json}", "Prices")
    .addTimeSeries("smaShort", "Prices")
    .addTimeSeries("smaLong", "Prices")
    .addTimeSeries("entry_exit", "Prices", _.setAxis(2))
    /*
    .addTimeSeries("fair_price_${fairPriceIndicator}", "Prices")
    .addTimeSeries("bid_1", "Prices", _.setFill(false).setColor("rgba(255,255,255,.2)"))
    .addTimeSeries("bid_2", "Prices", _.setFill(false).setColor("rgba(255,255,255,.2)"))
    .addTimeSeries("bid_3", "Prices", _.setFill(false).setColor("rgba(255,255,255,.2)"))
    .addTimeSeries("ask_1", "Prices", _.setFill(false).setColor("rgba(255,255,255,.2)"))
    .addTimeSeries("ask_2", "Prices", _.setFill(false).setColor("rgba(255,255,255,.2)"))
    .addTimeSeries("ask_3", "Prices", _.setFill(false).setColor("rgba(255,255,255,.2)"))
    */
    /*
    .addPanel("Equity")
    .addTimeSeries("equity", "Equity")
    .addTimeSeries("buy_and_hold", "Buy&Hold")
    */
    .addPanel("Balances", "Portfolio")
    .addTimeSeries("cash", "Balances")
    .addTimeSeries("available_balance", "Balances")
    .addTimeSeries("position", "Balances", _.setAxis(2))
    .addTimeSeries("order_margin", "Balances")
  )

}
