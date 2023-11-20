package bio.ferlab.datalake.testutils

import bio.ferlab.datalake.commons.config.{RunStep, RuntimeETLContext, RuntimeIdETLContext, RuntimeTimestampETLContext, SimpleConfiguration}
import org.apache.spark.sql.SparkSession


/**
 * Default Test ETL context is a timestamp-based ETL.
 */
class TestETLContext(steps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession)
  extends RuntimeETLContext("path", steps = Nil, appName = Some("Spark Test")) {
  override lazy val config: SimpleConfiguration = configuration
  override lazy val spark: SparkSession = sparkSession
  override lazy val runSteps: Seq[RunStep] = steps
}

object TestETLContext {
  def apply(runSteps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession): RuntimeETLContext = {
    new TestETLContext(runSteps)(configuration, sparkSession)
  }
}

/**
 * Timestamp-based ETL context for testing.
 */
class TestTimestampETLContext(steps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession)
  extends RuntimeTimestampETLContext("path", steps = Nil, appName = Some("Spark Test")) {
  override lazy val config: SimpleConfiguration = configuration
  override lazy val spark: SparkSession = sparkSession
  override lazy val runSteps: Seq[RunStep] = steps
}

object TestTimestampETLContext {
  def apply(runSteps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession): TestTimestampETLContext = {
    new TestTimestampETLContext(runSteps)(configuration, sparkSession)
  }
}

/**
 * Id-based ETL context for testing.
 */
class TestIdETLContext(steps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession)
  extends RuntimeIdETLContext("path", steps = Nil, appName = Some("Spark Test")) {
  override lazy val config: SimpleConfiguration = configuration
  override lazy val spark: SparkSession = sparkSession
  override lazy val runSteps: Seq[RunStep] = steps
}

object TestIdETLContext {
  def apply(runSteps: Seq[RunStep] = Nil)(implicit configuration: SimpleConfiguration, sparkSession: SparkSession): TestIdETLContext = {
    new TestIdETLContext(runSteps)(configuration, sparkSession)
  }
}