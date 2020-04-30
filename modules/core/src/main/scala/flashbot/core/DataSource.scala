package flashbot.core

import akka.NotUsed
import akka.actor.ActorContext
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import flashbot.core.DataSource.{IngestOne, IngestSchedule}
import flashbot.core.FlashbotConfig.ExchangeConfig

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class DataSource {

  var exchangeConfig: Option[ExchangeConfig] = None

  /**
    * All available topics for this data source.
    */
  def discoverTopics(exchangeConfig: Option[ExchangeConfig])
                    (implicit ctx: ActorContext, mat: ActorMaterializer): Future[Set[String]] =
    Future.successful(exchangeConfig.flatMap(_.pairs)
      .map(_.map(_.toString).toSet).getOrElse(Set.empty))

  /**
    * This informs the DataSourceActor which manages this DataSource instance about how to work
    * through the queue of topics that need to be ingested. It exists to allow data sources with
    * large amounts of topics to batch and throttle ingest. For example, Binance has hundreds of
    * markets (topics). It's not practical to open 200 WebSocket connections to their servers all
    * at once. To address this, the Binance DataSource will schedule batches of topics to be
    * ingested together, with a delay between each batch. Additionally, because of how the Binance
    * API works, all topics of each batch can be multiplexed over a single WebSocket, greatly
    * reducing resource usage.
    *
    * By default there is no grouping and no throttle. All topics are ingested immediately and
    * individually.
    */
  def scheduleIngest(topics: Set[String], dataType: String): IngestSchedule =
    IngestOne(topics.head, 0 seconds)

  /**
    * Ingests a batch of topics in one go. Returns a map of (topic -> data stream).
    */
  def ingestGroup[T](topics: Set[String], datatype: DataType[T])
                    (implicit ctx: ActorContext, mat: ActorMaterializer)
      : Future[Map[String, Source[(Long, T), NotUsed]]] =
    Future.failed(new NotImplementedError("ingestGroup is not implemented by this data source."))

  /**
    * Ingests a single topic. Returns the corresponding data stream.
    */
  def ingest[T](topic: String, datatype: DataType[T])
               (implicit ctx: ActorContext, mat: ActorMaterializer)
      : Future[Source[(Long, T), NotUsed]] =
    Future.failed(new NotImplementedError("ingest is not implemented by this data source."))

  /**
    * Given a cursor and a topic/type, returns a page of results for the path and a cursor to the
    * next reverse chronological page (i.e. with older data). The return type also includes a delay
    * to wait until the next page will be requested, in case throttling is necessary.
    *
    * Overriding methods should throw [[UnsupportedOperationException]] to signal to the engine
    * that it should not continue trying to backfill with the given parameters.
    */
  def backfillPage[T](topic: String, datatype: DataType[T], cursor: Option[String])
                     (implicit ctx: ActorContext, mat: ActorMaterializer)
      : Future[(Vector[(Long, T)], Option[(String, FiniteDuration)])] =
    Future.failed(new UnsupportedOperationException("This data source does not support backfills."))

  /**
    * If this data source emits a custom, non-built-in data type, the type needs to be declared
    * here by supplying a DeltaFmtJson.
    *
    * Warning: Do not use this yet. Custom data types are not fully supported at this time.
    */
  def types: Seq[DataType[_]] = Seq.empty

  protected[flashbot] def backfillTickRate: Double = 1d
}

object DataSource {

  sealed trait IngestSchedule {
    def delay: Duration
  }

  case class IngestGroup(topics: Set[String], delay: Duration) extends IngestSchedule
  case class IngestOne(topic: String, delay: Duration) extends IngestSchedule

  class Foo extends DataSource {
  }

}

