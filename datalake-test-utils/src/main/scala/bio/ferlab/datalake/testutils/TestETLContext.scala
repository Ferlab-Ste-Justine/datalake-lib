package bio.ferlab.datalake.testutils

import bio.ferlab.datalake.commons.config.{RunStep, RuntimeETLContext, SimpleConfiguration}
import org.apache.spark.sql.SparkSession

class TestETLContext(steps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession) extends RuntimeETLContext("path", steps = "", appName = Some("Spark Test")) {
  override lazy val config: SimpleConfiguration = configuration
  override lazy val spark: SparkSession = sparkSession
  override lazy val runSteps: Seq[RunStep] = steps
}

object TestETLContext {
  def apply(runSteps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession): RuntimeETLContext = {
    new TestETLContext(runSteps)(configuration, sparkSession)
  }
}