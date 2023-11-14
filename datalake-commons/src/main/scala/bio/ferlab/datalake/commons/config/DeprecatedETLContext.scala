package bio.ferlab.datalake.commons.config

import mainargs.{ParserForClass, arg}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j
import pureconfig.ConfigReader

@deprecated("use [[v2.ETLContext]] instead", "11.0.0")
trait DeprecatedETLContext[T <: Configuration] {
  def config: T

  def spark: SparkSession

  def runSteps: Seq[RunStep]
}

abstract class DeprecatedBaseETLContext[T <: Configuration](path: String, steps: String, appName: Option[String])(implicit cr: ConfigReader[T]) extends DeprecatedETLContext[T] {
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

case class DeprecatedRuntimeETLContext(
                              @arg(name = "config", short = 'c', doc = "Config path") path: String,
                              @arg(name = "steps", short = 's', doc = "Steps") steps: String,
                              @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                            ) extends DeprecatedBaseETLContext[SimpleConfiguration](path, steps, appName) {

}

object DeprecatedRuntimeETLContext {
  implicit def configParser: ParserForClass[DeprecatedRuntimeETLContext] = ParserForClass[DeprecatedRuntimeETLContext]
}

