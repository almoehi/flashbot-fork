package flashbot.core

import flashbot.models.Order._
import flashbot.models.OrderBook
import io.circe.Json
import org.scalatest.{FlatSpec, Matchers}

class DataModelSpec extends FlatSpec with Matchers {
  "Report" should "allow to create empty report" in {
    val r1 = Report.empty("test-strat", Json.Null)
    r1.targetAsset shouldBe "usd"

    val r2 = Report.empty("test-strat", Json.obj())
    r2.targetAsset shouldBe "usd"
  }
}
