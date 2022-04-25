package bio.ferlab.datalake.spark3.etl.v2

import org.scalatest.events.Event
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Args, Reporter}

trait ETLTest extends AnyFlatSpec with Matchers {self =>

  /**
   * Runs all tests in the test Suite and throws an exception if at least one test fails.
   */
  def runAll(): Unit = {
    val reporter = new Reporter() {
      override def apply(e: Event): Unit = {}
    }
    self.execute(stats = true)
    val testNames = self.testNames

    val failedTests: Set[Boolean] = testNames.map { test =>
      self.run(Some(test), Args(reporter)).succeeds()
    }.filterNot(identity)

    if (failedTests.nonEmpty) {
      throw new Exception(s"${failedTests.size} tests have failed.")
    }
  }

}