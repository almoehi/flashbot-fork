package flashbot.core

import org.scalatest.{FlatSpec, Matchers}
import flashbot.util.stream._

class StreamUtilsSpec extends FlatSpec with Matchers {
  "dropDuplicates" should "work" in {
    val vals = Seq(1, 2, 3, 3, 4, 5)
    vals.toStream.dropDuplicates.toList shouldEqual vals.distinct
  }
}
