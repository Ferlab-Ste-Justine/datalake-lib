package bio.ferlab.datalake.spark3.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class DeltaUtilsSpec  extends AnyFlatSpec with Matchers {

  "getRetentionHours" should "return the difference between now and the oldest timestamp - 1 hour" in {

    val clock = LocalDateTime.of(2020, 10, 9, 2, 30, 0)

    val oldestTimestamp = Timestamp.valueOf("2020-10-07 08:45:00")
    val timestamps = Seq(
      Timestamp.valueOf("2020-10-08 13:56:00"),
      oldestTimestamp,
      Timestamp.valueOf("2020-10-08 15:13:00")
    )
    val l = DeltaUtils.getRetentionHours(timestamps, clock)
    clock.minusHours(l).isBefore(oldestTimestamp.toLocalDateTime) shouldBe true
    l shouldBe 42

  }

}
