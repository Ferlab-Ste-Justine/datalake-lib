package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, RunStep}
import mainargs.{ParserForClass, arg}
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigReader

trait ETLContext {
  def config[T <: Configuration](implicit cr: ConfigReader[T]): T

  def spark: SparkSession

  def runSteps: Seq[RunStep]
}

case class RuntimeETLContext(
                              @arg(name = "config", short = 'c', doc = "Config path") path:
                              String, @arg(name = "steps", short = 's', doc = "Steps") steps: String
                            ) extends ETLContext {
  def config[T <: Configuration](implicit cr: ConfigReader[T]): T = ConfigurationLoader.loadFromResources[T](path)

  override val spark: SparkSession = SparkSession
    .builder
    .appName("Test")
    .master("local[*]")
    .getOrCreate()
  override val runSteps: Seq[RunStep] = RunStep.getSteps("default")
}

object RuntimeETLContext {
  implicit def configParser[T <: Configuration]: ParserForClass[RuntimeETLContext] = ParserForClass[RuntimeETLContext]
}