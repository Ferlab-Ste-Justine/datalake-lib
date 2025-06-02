package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.testutils.WithLogCapture
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorHandlingUtilsSpec extends AnyFlatSpec with Matchers with WithLogCapture {

  "runOptThrow" should "throw the first exception if any" in {
    val exception1 = new RuntimeException("First error")
    val exception2 = new RuntimeException("Second error")

    val thrown = the [RuntimeException] thrownBy {
      ErrorHandlingUtils.runOptThrow(
        Seq(None, Some(exception1), Some(exception2))
      )
    }
    thrown.getMessage should equal ("First error")
  }

  "runOptThrow" should "support a by-name parameter and throw the first exception if any" in {
    val exception1 = new RuntimeException("First error")
    val exception2 = new RuntimeException("Second error")

    val thrown = the [RuntimeException] thrownBy {
      ErrorHandlingUtils.runOptThrow {
        Seq(None, Some(exception1), Some(exception2))
      }
    }
    thrown.getMessage should equal ("First error")
  }

  "runOptThrow" should "not throw if no exceptions are present" in {
    noException should be thrownBy {
      ErrorHandlingUtils.runOptThrow(Seq(None, None))
    }
  }

  "runOptThrow" should "do nothing if the iterable is empty" in {
    noException should be thrownBy {
      ErrorHandlingUtils.runOptThrow(Seq.empty)
    }
  }

  "optIfFailed" should "return None if the function succeeds" in {
    withLogCapture(ErrorHandlingUtils.getClass) { appender => 
      def successfulFunction(): Int = 42
      val result = ErrorHandlingUtils.optIfFailed(successfulFunction, "This should not be logged")

      result shouldBe None
      appender.getEvents shouldBe empty
    }
  }

  "optIfFailed" should "return Some(Throwable) if the function fails" in {
    withLogCapture(ErrorHandlingUtils.getClass) { appender => 
      def failingFunction(): Int = throw new RuntimeException("Test error")

      val result = ErrorHandlingUtils.optIfFailed(failingFunction, "This should be logged")

      result shouldBe defined
      result.get.getMessage shouldBe "Test error"
      appender.getEvents.size shouldBe 1
      appender.getEvents.head.getMessage.getFormattedMessage() shouldBe "This should be logged"
    }
  }


}
