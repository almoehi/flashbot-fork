package flashbot.server
import java.time.Instant

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import flashbot.core.DataServer.LiveStream
import flashbot.core.FlashbotConfig.{DataSourceConfig, IngestConfig}
import flashbot.core._
import flashbot.core.DataSource
import flashbot.core.DataSource._
import flashbot.models.{DataSelection, DataStreamReq, Ladder, TakeFirst, TakeLast, TakeLimit}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import flashbot.sources.TestBackfillDataSource
import util.TestDB

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class DataServerSpec extends WordSpecLike with Matchers with Eventually {


  val exchange = "coinbase"
  val configName = "test" //use test.conf

  "DataServer" should {

    "ingest and serve trades" in {
      implicit val config = FlashbotConfig.loadStandalone(configName).copy(
        ingest = IngestConfig(
          enabled = Seq(s"$exchange/btc_usd/trades"),
          backfill = Seq(Seq(s"$exchange/btc_usd/trades")),
          retention = Seq(Seq("*/*/trades", "10h"))
        )
      )

      implicit val system = ActorSystem(config.systemName, config.conf)

      implicit val timeout = Timeout(10 seconds)
      implicit val mat = ActorMaterializer()
      implicit val ec = system.dispatcher

      println(s"DB setup: ${config.db}")

      // Create data server actor.
      val dataserver = system.actorOf(Props(new DataServer(config.db,
        // Ingests from a stream that is configured to send data for about 3 seconds.
        Map(exchange -> DataSourceConfig("flashbot.sources.TestDataSource", Some(Seq("trades")))),
        config.exchanges,
        IngestConfig(Seq(s"$exchange/btc_usd/trades"), Seq(), Seq(Seq()))
      )))

      // Ingest for 2 second with no subscriptions.
      Thread.sleep(8000)

      // Then subscribe to a path and series a data stream.
      // TODO: does not work with DataStreamReq
      val fut = dataserver ? DataStreamReq(DataSelection(s"$exchange/btc_usd/trades", Some(0))) // LiveStream(s"$exchange/btc_usd/trades")
      val rsp = Await.result(fut.mapTo[StreamResponse[MarketData[Trade]]], timeout.duration)

      //val rsp = Await.result(fut.mapTo[StreamResponse[MarketData[Trade]]], timeout.duration)
      val rspStream = rsp.toSource

      val mds = Await.result(rspStream.toMat(Sink.seq)(Keep.right).run, timeout.duration)
      val expectedIds = (1 to 120).map(_.toString)
      println(mds.map(s => (s.bundle,s.seqid,s.data.id)))
      println("Expected")
      println(expectedIds)
      println("DataServer")
      println(mds.map(_.data.id))
      mds.map(_.data.id) shouldEqual expectedIds.take(mds.size)

      Await.ready(for {
        _ <- system.terminate()
        _ <- TestDB.dropTestDB()
      } yield Unit, 10 seconds)
    }

/*
    "ingest and serve ladders" in {
      implicit val config = FlashbotConfig.load(configName)
      implicit val system = ActorSystem(config.systemName, config.conf)

      implicit val timeout = Timeout(1 minute)
      implicit val mat = ActorMaterializer()
      implicit val ec = system.dispatcher

      // Create data server actor.
      val dataserver = system.actorOf(Props(new DataServer(config.db,
        Map(exchange -> DataSourceConfig("flashbot.sources.TestLadderDataSource", Some(Seq("ladder")))),
        config.exchanges,
        IngestConfig(Seq(s"$exchange/btc_usd/ladder"), Seq(), Seq(Seq()))
      )))

      // Ingest for 2 second with no subscriptions.
      Thread.sleep(2000)

      // Then subscribe to a path and series a data stream.
      val fut = dataserver ? DataStreamReq(DataSelection(s"$exchange/btc_usd/ladder", Some(0)))
      val rsp = Await.result(fut.mapTo[StreamResponse[MarketData[Ladder]]], timeout.duration)
      val rspStream = rsp.toSource

      Await.ready(for {
        _ <- system.terminate()
        _ <- TestDB.dropTestDB()
      } yield Unit, 10 seconds)
    }
*/
    /**
      * Three separate data sources are used to simulate real world conditions:
      *
      *   * Data source A backfills from 0 to 150 and live streams from 150 to 200.
      *   * Data source B backfills from 300 to 450 and live streams from 450 to 500.
      *     It crashes when after it reaches 300. Leaving a gap between 200 and 300.
      *   * Data source C backfills from 500 to 900 and live streams from 900 to 1000.
      *     It also resumes where B left off, backfilling 200 to 300.
      *
      *                   <-----> - - - - - <------- C -------|----->
      *       <---- A -|-->     x---- B -|-->
      * <-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|--->
      *       0    100      ...            500      ...      900   1000
      */


    "ingest and backfill trades" in {

      import TestBackfillDataSource._

      implicit val timeout = Timeout(1 minute)
      implicit val _config = FlashbotConfig.loadStandalone(configName).copy(
        ingest = IngestConfig(
          enabled = Seq(s"$exchange/btc_usd/trades"),
          backfill = Seq(Seq(s"$exchange/btc_usd/trades")),
          retention = Seq(Seq("*/*/trades", "10h"))
        )
      )

      def runDataServer(dataSourceName: String, assert: Seq[MarketData[Trade]] => Unit): Unit = {

        val config = _config.copy(
          sources = Map(
            exchange -> DataSourceConfig(dataSourceName, Some(Seq("trades")))))

        implicit val system = ActorSystem(config.systemName, config.conf)
        val dataServer = system.actorOf(DataServer.props(config))

        implicit val mat = ActorMaterializer()
        implicit val ec = system.dispatcher

        def fetchTrades() = {
          val fut = dataServer ? DataStreamReq(DataSelection(
            s"$exchange/btc_usd/trades", Some(0), Some(Long.MaxValue)))
          val rsp = Await.result(fut.mapTo[StreamResponse[MarketData[Trade]]], timeout.duration)
          val src = rsp.toSource
          Await.result(src.toMat(Sink.seq)(Keep.right).run, timeout.duration)
        }

        implicit val patienceConfig =
          PatienceConfig(timeout = scaled(Span(8, Seconds)), interval = scaled(Span(500, Millis)))

        eventually {
          assert(fetchTrades())
        }

        Await.ready(system.terminate(), 10 seconds)
      }

      runDataServer("flashbot.sources.TestBackfillDataSourceA", fetched => {
        fetched.map(_.data) shouldEqual (historicalTradesA ++ liveTradesA)
      })

      runDataServer("flashbot.sources.TestBackfillDataSourceB", fetched => {
        fetched.map(_.data) shouldEqual
          ((historicalTradesA ++ liveTradesA) ++
            (historicalTradesB ++ liveTradesB))
      })

      runDataServer("flashbot.sources.TestBackfillDataSourceC", fetched => {
        fetched.map(_.data) shouldEqual allTradesAfterRetention
      })

      Await.ready(TestDB.dropTestDB(), 10 seconds)
    }


    "aggregate various data types, live and historical, into a continuous dataset of candles" in {
      // Specifically, we're testing the Coinbase DataSource here, which backfills 1m candles
      // straight from the exchange, but uses live trades to build the candles ingest stream.
      // Should handle volume correctly as well.
    }
  }
}
