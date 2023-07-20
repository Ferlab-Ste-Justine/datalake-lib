package bio.ferlab.datalake.spark3.etl.v3

import bio.ferlab.datalake.commons.config.{Configuration, RunStep}
import bio.ferlab.datalake.spark3.etl.ETLContext
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigReader

case class TestETLContext(override val runSteps: Seq[RunStep] = Nil)(implicit val spark:SparkSession, conf:Configuration) extends ETLContext {
  override def config[T <: Configuration](implicit cr: ConfigReader[T]): T = conf.asInstanceOf[T]


}