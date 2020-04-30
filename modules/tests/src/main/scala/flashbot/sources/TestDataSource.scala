package flashbot.sources
import akka.NotUsed
import akka.actor.ActorContext
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import flashbot.core.DataType.TradesType
import flashbot.core._
import flashbot.core.DataType
import flashbot.models.Order._

import scala.concurrent.Future
import scala.concurrent.duration._

class TestDataSource extends DataSource {

  val MicrosPerMinute: Long = 60L * 1000000

  override def ingest[T](topic: String, datatype: DataType[T])
                        (implicit ctx: ActorContext, mat: ActorMaterializer) = datatype match {
    case TradesType =>
      val xs = (1 to 120)
      val nowMicros = System.currentTimeMillis() * 1000
      println(s"TestDataSource: Ingesting $xs")
      val src: Source[(Long, T), NotUsed] = Source(xs map { i =>
        Trade(i.toString, nowMicros + i * MicrosPerMinute, i, i, if (i % 2 == 0) Up else Down)
      }) map (t => (t.micros, t.asInstanceOf[T]))
      Future.successful(src.throttle(1, 100 millis))
  }
}
