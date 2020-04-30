package flashbot.core

import java.time.Instant

import com.typesafe.config.{Config, ConfigFactory}
import flashbot.models._
import flashbot.util.time
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.generic.extras._
import io.circe.config.syntax._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import FlashbotConfig._

@ConfiguredJsonCodec(decodeOnly = true)
case class FlashbotConfig(engineRoot: String,
                          systemName: String,
                          ingest: IngestConfig,
                          strategies: Map[String, String],
                          exchanges: Map[String, ExchangeConfig],
                          sources: Map[String, DataSourceConfig],
                          bots: StaticBotsConfig,
                          grafana: GrafanaConfig,
                          conf: Config,
                          db: Config) {
  def noIngest: FlashbotConfig = copy(ingest = ingest.copy(enabled = Seq.empty))
}

object FlashbotConfig {
  implicit val jsonConfig: Configuration = Configuration.default.withKebabCaseMemberNames

  implicit val durationDecoder: Decoder[FiniteDuration] = time.DurationDecoder

  val DefaultTTL: FiniteDuration = 0 seconds

  @ConfiguredJsonCodec(decodeOnly = true)
  case class IngestConfig(enabled: Seq[String], backfill: Seq[Seq[String]], retention: Seq[Seq[String]], snapshotInterval: FiniteDuration = 4 hours) {
    def ingestMatchers: Set[DataPath[Any]] = enabled.toSet.map(DataPath.parse)
    def backfillMatchers: Set[DataPath[Any]] = backfill.filter(_.nonEmpty).map(_.head).toSet.map(DataPath.parse)

    def backfillPeriodFor(path: DataPath[_]): Option[Instant] = {
      backfill.filter(_.nonEmpty).find(record => DataPath(record.head).matches(path)).filter(_.size > 1).filter(_.last != "*")
        .map(record => time.parseDuration(record.last)).map(dt => Instant.now().minusSeconds(dt.toSeconds))
    }

    def filterIngestSources(sources: Set[String]): Set[String] = sources.filter(src =>
      ingestMatchers.exists(_.matches(s"$src/*/*")))
    def filterBackfillSources(sources: Set[String]): Set[String] = sources.filter(src =>
      backfillMatchers.exists(_.matches(s"$src/*/*")))
    def filterSources(sources: Set[String]): Set[String] =
      filterIngestSources(sources) ++ filterBackfillSources(sources)

    def retentionFor(path: DataPath[_]): Option[FiniteDuration] = {
      retention.filter(_.nonEmpty).find(record => DataPath(record.head).matches(path))
        .map(record => time.parseDuration(record.last))
    }
  }

  @ConfiguredJsonCodec(decodeOnly = true)
  case class ExchangeConfig(`class`: String, params: Option[Json], pairs: Option[Seq[String]], apiCredentials: Option[ExchangeApiCredentials]) {
    def sandboxApiUrl: Option[String] = params.flatMap(_.hcursor.downField("sandbox").downField("api").as[String].toOption).map(_.stripSuffix("/"))
    def sandboxWebsocketUrl: Option[String] = params.flatMap(_.hcursor.downField("sandbox").downField("websocket").as[String].toOption).map(_.stripSuffix("/"))
    def hasSandboxServer: Boolean = params.map(_.hcursor.downField("sandbox").keys.isDefined).getOrElse(false)

    def liveApiUrl: Option[String] = params.flatMap(_.hcursor.downField("live").downField("api").as[String].toOption).map(_.stripSuffix("/"))
    def liveWebsocketUrl: Option[String] = params.flatMap(_.hcursor.downField("live").downField("websocket").as[String].toOption).map(_.stripSuffix("/"))
    def hasLiveServer: Boolean = params.map(_.hcursor.downField("live").keys.isDefined).getOrElse(false)

    def isLive: Boolean = params.flatMap(_.hcursor.get[Boolean]("useLive").toOption).getOrElse(false)
    def apiUrls: Option[(String,String)] = isLive match {
      case true => Some((liveApiUrl,liveWebsocketUrl)).filter(t => t._1.isDefined && t._2.isDefined).map(t => (t._1.get,t._2.get))
      case _ => Some((sandboxApiUrl,sandboxWebsocketUrl)).filter(t => t._1.isDefined && t._2.isDefined).map(t => (t._1.get,t._2.get))
    }
  }

  @ConfiguredJsonCodec(decodeOnly = true)
  case class ExchangeApiCredentials(userName: String, apiKey: String, apiSecret: String)

  @ConfiguredJsonCodec(decodeOnly = true)
  case class BotConfig(strategy: String,
                       mode: TradingSessionMode,
                       params: Json = Json.obj(),
                       ttl: FiniteDuration = DefaultTTL,
                       initialAssets: Map[String, Double] = Map.empty,
                       initialPositions: Map[String, Position] = Map.empty) {
    def ttlOpt: Option[Duration] = ttl match {
      case DefaultTTL => None
      case other => Some(other)
    }
  }

  @ConfiguredJsonCodec(decodeOnly = true)
  case class StaticBotsConfig(enabled: Seq[String], configs: Map[String, BotConfig]) {
    def enabledConfigs: Map[String, BotConfig] = configs.filterKeys(enabled contains _)
  }

  @ConfiguredJsonCodec(decodeOnly = true)
  case class GrafanaConfig(dataSource: Boolean, dataSourcePort: Int,
                           host: String, apiKey: Option[String], requestTimeout: Option[FiniteDuration] = Some((60 seconds)))

  @ConfiguredJsonCodec(decodeOnly = true)
  case class DataSourceConfig(`class`: String, datatypes: Option[Seq[String]])




  def tryLoad(key: String, appName: String, standalone: Boolean): Try[FlashbotConfig] = {
    val overrides = ConfigFactory.defaultOverrides()
    val apps = ConfigFactory.parseResources(s"$appName.conf")
    val refs = ConfigFactory.parseResources("reference.conf")

    var fbAkkaConf = refs.getConfig(key).withOnlyPath("akka")
    if (!standalone) {
      fbAkkaConf = refs.getConfig(s"$key.akka-cluster").atKey("akka").withFallback(fbAkkaConf)
    }

    var conf = overrides
      // Initial fallback is to `application.conf`
      .withFallback(apps)
      // We want `flashbot.akka` reference to override `akka` reference.
      .withFallback(fbAkkaConf)
      // Then we fallback to default references.
      .withFallback(refs.withoutPath(s"$key.akka").withoutPath(s"$key.akka-cluster"))
      // Finally, resolve.
      .resolve()

    // Replace db with the chosen config.
    val dbOverride = conf.getConfig(conf.getString(s"$key.db"))
    conf = dbOverride.atPath(s"$key.db").withFallback(conf)

    // Place full copy of the config into "flashbot.conf".
    conf = conf.withoutPath(key).atPath(s"$key.conf").withFallback(conf)

    // Decode into a FlashbotConfig
    conf.getConfig(key).as[FlashbotConfig].toTry
  }

  def load(appName: String = "application"): FlashbotConfig =
    tryLoad("flashbot", appName, standalone = false).get

  def loadStandalone(appName: String = "application"): FlashbotConfig =
    tryLoad("flashbot", appName, standalone = true).get
}

