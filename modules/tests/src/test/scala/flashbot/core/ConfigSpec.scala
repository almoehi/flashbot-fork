package flashbot.core

import org.scalatest.{FlatSpec, FunSpec, FunSuite, Matchers}

import scala.concurrent.duration.Duration

class ConfigSpec extends FlatSpec with Matchers {
  val configName = "test"
  "FlashbotConfig" should "load the default config" in {
    val config = FlashbotConfig.load(configName)
    config.systemName shouldBe "flashbot-system"
    config.db.getString("profile") shouldBe "slick.jdbc.H2Profile$"
    config.engineRoot shouldBe "target/engines"
    config.conf.getString("akka.actor.provider") shouldBe "akka.cluster.ClusterActorRefProvider"
  }

  "FlashbotConfig" should "load in standalone mode" in {
    val config = FlashbotConfig.loadStandalone(configName)
    config.conf.getString("akka.actor.provider") shouldBe "local"
  }

  "FlashbotConfig" should "load postgres db settings" in {
    val config = FlashbotConfig.load("prod")
    config.db.getString("profile") shouldBe "slick.jdbc.PostgresProfile$"

    config.db.getString("db.properties.databaseName") shouldBe "postgres_test"
  }

  "FlashbotConfig" should "allow overwriting Akka options" in {
    val config = FlashbotConfig.load("custom-akka")
    config.conf.getString("akka.loglevel") shouldBe "ERROR"
    config.conf.getString("akka.cluster.log-info") shouldBe "off"
  }

  "FlashbotConfig" should "support includes" in {
    // No include here
    val noInclude = FlashbotConfig.load("custom-akka")
    noInclude.engineRoot shouldBe FlashbotConfig.load("non-existent-app").engineRoot
    noInclude.conf.getInt("akka.port") shouldBe 2551

    // Includes "application"
    val config = FlashbotConfig.load("custom-port")
    config.engineRoot shouldBe "target/engines"
    config.conf.getInt("akka.port") shouldBe 2555
  }

  "FlashbotConfig" should "support grafana config" in {
    val config = FlashbotConfig.load(configName)
    val g = config.grafana
    g.requestTimeout shouldBe Some(Duration("2m"))
    g.dataSource shouldBe true
    g.host shouldBe "http://localhost:3000"
    g.dataSourcePort shouldBe 3002

    g.backtest.defaultReportTargetAsset shouldBe Some("eur")
    g.backtest.defaultMarket shouldBe Some("coinbase.eth_eur")
    g.backtest.defaultPortfolio shouldBe Some("coinbase.eur=2000")
  }
}
