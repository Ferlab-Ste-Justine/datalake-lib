package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, RunStep, SimpleConfiguration}
import mainargs.{ParserForClass, arg}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j
import pureconfig.ConfigReader

trait ETLContext[T <: Configuration] {
  def config: T

  def spark: SparkSession

  def runSteps: Seq[RunStep]
}

abstract class BaseETLContext[T <: Configuration](path: String, steps: String, appName: Option[String])(implicit cr: ConfigReader[T]) extends ETLContext[T] {
  private val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)
  log.info(s"Loading config file: [$path]")

  lazy val config: T = ConfigurationLoader.loadFromResources[T](path)

  lazy val sparkConf: SparkConf = this.config.sparkconf.foldLeft(new SparkConf()) { case (c, (k, v)) => c.set(k, v) }

  lazy val runSteps: Seq[RunStep] = RunStep.getSteps(steps)

  lazy val spark: SparkSession =
    SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .appName(appName.getOrElse("SparkApp"))
      .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

}

case class RuntimeETLContext(
                              @arg(name = "config", short = 'c', doc = "Config path") path: String,
                              @arg(name = "steps", short = 's', doc = "Steps") steps: String,
                              @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                            ) extends BaseETLContext[SimpleConfiguration](path, steps, appName) {

}

object RuntimeETLContext {
  implicit def configParser[T <: Configuration]: ParserForClass[RuntimeETLContext] = ParserForClass[RuntimeETLContext]
}

