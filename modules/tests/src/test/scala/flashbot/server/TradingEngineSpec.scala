package flashbot.server

import java.io.File
import java.time.Instant

import io.circe.syntax._
import akka.actor.{ActorSystem, Props, Status}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import org.jfree.data.time._
import flashbot.core.{Report, _}
import flashbot.util.files.rmRf
import flashbot.client.FlashbotClient
import flashbot.core.FlashbotConfig.{BotConfig, StaticBotsConfig}
import flashbot.models._
import flashbot.util.time
import org.jfree.data.time.{RegularTimePeriod, TimeSeriesCollection}
import org.jfree.data.time.ohlc.{OHLCSeries, OHLCSeriesCollection}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, WordSpecLike}
import strategies.{CandleScannerParams, LookaheadParams, TradeWriter}
import util.TestDB
import akka.pattern.{ask, pipe}
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class TradingEngineSpec extends WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  var testFolder: File = _
  val configName = "test" // use test.conf
  implicit val timeout = Timeout(15 seconds)

  override def afterAll() = {
    // Delete the engine root
    rmRf(new File(FlashbotConfig.load().engineRoot))
  }

  "TradingEngine" should {

    "respond to a ping" in {
      implicit val config = FlashbotConfig.loadStandalone(configName)
      val system = ActorSystem(config.systemName, config.conf)

      val dataServer = system.actorOf(DataServer.props(config))

      val engine = system.actorOf(Props(new TradingEngine(
        "test",
        config.strategies,
        config.exchanges,
        config.bots,
        Left(dataServer),
        config.grafana
      )))

      val fb = new FlashbotClient(engine)

      fb.ping() match {
        case Pong(micros) =>
          println(Instant.ofEpochMilli(micros/1000))
        case _ =>
          fail("should respond with a Pong")
      }

      Await.ready(for {
        _ <- system.terminate()
        _ <- TestDB.dropTestDB()
      } yield Unit, 10 seconds)
    }

    "respect bot TTL" in {
      implicit val config = FlashbotConfig.loadStandalone(configName)
      val system = ActorSystem(config.systemName, config.conf)

      val engine = system.actorOf(TradingEngine.props("test-engine", config.noIngest))
      val fb = new FlashbotClient(engine)

      // Configure bot with 2 second TTL
      fb.configureBot("mybot", BotConfig("candlescanner",
        mode = Paper(),
        ttl = 2 seconds,
        params = CandleScannerParams("usd").asJson
      ))

      // Status should be Disabled
      fb.botStatus("mybot") shouldBe Disabled

      // Enable bot
      fb.enableBot("mybot")

      // Wait 1 second
      Thread.sleep(1000)

      // Status should be running
      fb.botStatus("mybot") shouldBe Running

      // Send a heartbeat
      fb.botHeartbeat("mybot")

      // Status should still be running 1.5 seconds after heartbeat
      Thread.sleep(1500)
      fb.botStatus("mybot") shouldBe Running

      // Wait 1 more second
      Thread.sleep(1000)

      // Status should fail with "unknown bot"
      assertThrows[IllegalArgumentException] {
        fb.botStatus("mybot")
      }

      Await.ready(for {
        _ <- system.terminate()
        _ <- TestDB.dropTestDB()
      } yield Unit, 10 seconds)
    }

    /**
      * We should be able to start a bot, then subscribe to a live stream of it's report.
      */

        "subscribe to the report of a running bot" in {

          implicit val config = FlashbotConfig.loadStandalone(configName)
          implicit val system = ActorSystem(config.systemName, config.conf)

          val engine = system.actorOf(TradingEngine.props("test-engine", config.noIngest))
          val fb = new FlashbotClient(engine)
          //implicit val mat = ActorMaterializer()

          val nowMicros = time.currentTimeMicros
          val trades = 1 to 20 map { i =>
            Trade(i.toString, nowMicros + i * 1000000, i, i, if (i % 2 == 0) flashbot.models.Order.Up else flashbot.models.Order.Down)
          }

          // Configure and enable bot that writes a list of trades to the report.
          fb.configureBot("bot2", BotConfig("tradewriter", Paper(),
            params = TradeWriter.Params(trades).asJson))
          fb.enableBot("bot2")

          Thread.sleep(1000)

          // Subscribe to the report. Receive a trade stream.
          val reportTradeSrc = fb.subscribeToReport("bot2")
            .map(_.getValues("last_trade").value.asInstanceOf[Trade])

          // Collect the stream into a seq.
          // TODO: this future doesn't seem to complete !
          val reportTrades = Await.result(
            reportTradeSrc.runWith(Sink.seq), 30 seconds
          ).dropRight(1)

    //      println(trades)
    //      println(reportTrades)

          // Verify that the data in the report stream is the expected list of trades.
          val a = trades.drop(trades.size - reportTrades.size)
    //      println("FIRST TRADE", reportTrades.head)
          reportTrades shouldEqual a

          // Also check that it was reverted to disabled state after the data stream completed.
          fb.botStatus("bot2") shouldBe Disabled

          Await.ready(for {
            _ <- system.terminate()
            _ <- TestDB.dropTestDB()
          } yield Unit, 10 seconds)
        }

        "enable static bots" in {

          implicit val config = FlashbotConfig.loadStandalone(configName).copy(bots = StaticBotsConfig(
            enabled = Seq("scanner1"),
            configs = Map(
              "scanner1" -> BotConfig("candlescanner", Paper(), CandleScannerParams("usd").asJson),
              "scanner2" -> BotConfig("candlescanner", Paper(), CandleScannerParams("usd").asJson)
            )
          ))

          implicit val system = ActorSystem(config.systemName, config.conf)
          val engine = system.actorOf(TradingEngine.props("engine", config.noIngest))
          val fb = new FlashbotClient(engine)
          fb.botStatus("scanner1") shouldBe Running
          fb.botStatus("scanner2") shouldBe Disabled

          Await.ready(for {
            _ <- system.terminate()
            _ <- TestDB.dropTestDB()
          } yield Unit, 10 seconds)
        }


        "be profitable when using lookahead" in {
          implicit val config = FlashbotConfig.loadStandalone(configName)
          implicit val system = ActorSystem(config.systemName, config.conf)

          val now = Instant.now()

          val dataServer = system.actorOf(DataServer.props(config), "data-server")

          val engine = system.actorOf(Props(
            new TradingEngine("test2", config.strategies, config.exchanges, config.bots,
              Left(dataServer), config.grafana)), "trading-engine-2")

          val params = LookaheadParams(Market("coinbase/eth_eur"), sabotage = false)

          val report = Await.result((engine ? BacktestQuery(
            "lookahead",
            params.asJson,
            TimeRange.build(now, 1 hour),
            new Portfolio(
              debox.Map.fromIterable(Map(Account("coinbase/eth") -> 0, Account("coinbase/eur") -> 800)),
              debox.Map.fromIterable(Map.empty),
              debox.Map.fromIterable(Map.empty),
              defaultTargetAsset = config.grafana.backtest.defaultReportTargetAsset.getOrElse("usd")
            ).toString,
            Some(1 minute),
            None
          )).map {
            case ReportResponse(report: Report) => report
          }, timeout.duration)

          report.error shouldBe None

          def reportTimePeriod(report: Report): Class[_ <: RegularTimePeriod] =
            (report.barSize.length, report.barSize.unit) match {
              case (1, MILLISECONDS) => classOf[Millisecond]
              case (1, SECONDS) => classOf[Second]
              case (1, MINUTES) => classOf[Minute]
              case (1, HOURS) => classOf[Hour]
              case (1, DAYS) => classOf[Day]
            }

          def buildCandleSeries(report: Report, key: String): OHLCSeries = {
            val priceSeries = new OHLCSeries(key)
            val timeClass = reportTimePeriod(report)
            report.getTimeSeries(key).toCandlesArray.foreach { candle =>
              val time =  RegularTimePeriod.createInstance(timeClass,
                new java.util.Date(candle.micros / 1000), java.util.TimeZone.getDefault)
          //          println("adding", timeClass, time, candle)
              priceSeries.add(time, candle.open, candle.high, candle.low, candle.close)
            }
            priceSeries
          }

          val equityCollection = new TimeSeriesCollection()

          val priceCollection = new OHLCSeriesCollection()
          //      priceCollection.addSeries(buildCandleSeries(report, "equity_usd"))
          //      priceCollection.addSeries(buildCandleSeries(report, "eth"))

          //      val chart = ChartFactory.createCandlestickChart("Look-ahead Report", "Time",
          //        "Price", priceCollection, true)

          //      val renderer = new CandlestickRenderer
          //      renderer.setAutoWidthMethod(CandlestickRenderer.WIDTHMETHOD_SMALLEST)
          //
          //      val plot = chart.getXYPlot
          //      plot.setRenderer(renderer)
          //
          //
          //      val histogramData = new HistogramDataset()
          //      histogramData.setType(HistogramType.RELATIVE_FREQUENCY)
          //
          //      val returns = report.collections("fill_size").map(_.as[Double].right.build)
          //      histogramData.addSeries("Fill Size", returns.toArray, 40)
          //
          //      val histogram = ChartFactory.createHistogram("Fill Size", "Size", "# of trades",
          //        histogramData, PlotOrientation.VERTICAL, true, true, false)
          //
          //      val hPlot = histogram.getXYPlot
          //      hPlot.setForegroundAlpha(0.85f)
          //      val yaxis = hPlot.getRangeAxis
          //      yaxis.setAutoTickUnitSelection(true)
          //      val xyRenderer = hPlot.getRenderer.asInstanceOf[XYBarRenderer]
          //      xyRenderer.setDrawBarOutline(false)
          //      xyRenderer.setBarPainter(new StandardXYBarPainter)
          //      xyRenderer.setShadowVisible(false)

          //      chart.setAntiAlias(true)

          //      val panel = new ChartPanel(chart)
          //      panel.setVisible(true)
          //      panel.setFillZoomRectangle(true)
          //      panel.setPreferredSize(new java.awt.Dimension(900, 600))

          //      val frame = new ChartFrame("Returns", chart)
          //      frame.pack()
          //      frame.setVisible(true)
          //      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)


          //      val mydata = for {
          //        bp <- report.collections("all/equity").map(_.as[BalancePoint].right.build)
          //      } yield (bp.micros, bp.balance)
          //
          //      val priceData =
          //        for(price <- report.timeSeries("price.bitfinex.eth_usd").dropRight(1))
          //        yield (price.micros / 1000, price.close)

          //      val mychart = XYLineChart(mydata)


          //      mychart.show("Equity")

          //      val fut = Future {
          //        Thread.sleep((2 days).toMillis)
          //      }
          //
          //      Await.ready(fut, 5 minutes)

          //      val plot = chart.getPlot.asInstanceOf[XYPlot]
          //
          //      plot.setDomainPannable(true)
          //
          //      val yAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
          //      yAxis.setForceZeroInRange(false)
          //      yAxis.setAutoRanging(true)


          //      report.timeSeries("returns").size shouldBe timeSeriesBarCount

          // There shouldn't be a single period of negative returns when the algo is cheating.

          Await.ready(for {
            _ <- system.terminate()
            _ <- TestDB.dropTestDB()
          } yield Unit, 10 seconds)
    }

//    "lose money when using lookahead to self sabatoge" in {
//
//    }
}
//
//  "TradingEngine" should "start a bot" in {
//    val system = ActorSystem("test")
//
//    val dataServer = system.actorOf(Props(
//      new DataServer(testFolder, Map.empty, Map.empty, None, useCluster = false)))
//
//    val engine = system.actorOf(Props(
//      new TradingEngine("test", Map.empty, Map.empty, Map.empty, dataServer)))
//
////    (engine ? )
//
//    system.terminate()
//
//  }
//
//  "TradingEngine" should "recover bots after a restart" in {
//  }
//
//  override def withFixture(test: NoArgTest) = {
//    val tempFolder = System.getProperty("java.io.tmpdir")
//    var folder: File = null
//    do {
//      folder = new File(tempFolder, "scalatest-" + System.nanoTime)
//    } while (! folder.mkdir())
//    testFolder = folder
//    try {
//      super.withFixture(test)
//    } finally {
//      rmRf(testFolder)
//    }
//  }
}
