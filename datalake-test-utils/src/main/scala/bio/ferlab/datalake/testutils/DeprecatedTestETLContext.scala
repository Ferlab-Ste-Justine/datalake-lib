package bio.ferlab.datalake.testutils

import bio.ferlab.datalake.commons.config.{DeprecatedRuntimeETLContext, RunStep, SimpleConfiguration}
import org.apache.spark.sql.SparkSession

@deprecated("use [[v2.TestETLContext]] instead", "11.0.0")
class DeprecatedTestETLContext(steps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession) extends DeprecatedRuntimeETLContext("path", steps = "", appName = Some("Spark Test")) {
  override lazy val config: SimpleConfiguration = configuration
  override lazy val spark: SparkSession = sparkSession
  override lazy val runSteps: Seq[RunStep] = steps
}

@deprecated("use [[v2.TestETLContext]] instead", "11.0.0")
object DeprecatedTestETLContext {
  def apply(runSteps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession): DeprecatedRuntimeETLContext = {
    new DeprecatedTestETLContext(runSteps)(configuration, sparkSession)
  }
}