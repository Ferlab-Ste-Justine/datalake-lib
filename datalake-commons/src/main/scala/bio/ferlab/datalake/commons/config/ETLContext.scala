package bio.ferlab.datalake.commons.config

import mainargs.{ParserForClass, arg}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j
import pureconfig.ConfigReader

import java.time.LocalDateTime
import scala.reflect.{ClassTag, classTag}

/**
 * Context for an ETL job.
 *
 * @tparam T Type used to capture data changes in the ETL
 * @tparam C Configuration type
 */
trait ETLContext[T, C <: Configuration] {
  def config: C

  def spark: SparkSession

  def runSteps: Seq[RunStep]

  /**
   * Tag of the ETL type. Used to overcome type erasure at runtime to allow pattern matching.
   */
  def ETLType: ClassTag[T]

  /**
   * Min value of the data type used to capture changes
   */
  def dataMinValue: T

  /**
   * Default current value for the data type used to capture changes
   */
  def defaultDataCurrentValue: T
}

abstract class BaseETLContext[T, C <: Configuration](path: String, steps: String, appName: Option[String])(implicit cr: ConfigReader[C])
  extends ETLContext[T, C] {
  private val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)
  log.info(s"Loading config file: [$path]")

  lazy val config: C = ConfigurationLoader.loadFromResources[C](path)

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

/**
 * Context for timestamp-based ETL. This is the default context type for ETLs.
 */
abstract class TimestampETLContext[C <: Configuration](path: String, steps: String, appName: Option[String])(implicit cr: ConfigReader[C])
  extends BaseETLContext[LocalDateTime, C](path, steps, appName) {

  override val ETLType: ClassTag[LocalDateTime] = classTag[LocalDateTime]
  override val dataMinValue: LocalDateTime = LocalDateTime.of(1900, 1, 1, 0, 0, 0)

  def defaultDataCurrentValue: LocalDateTime = LocalDateTime.now() // Use def to evaluate on call
}

/**
 * Context for ID-based ETL.
 */
abstract class IdETLContext[C <: Configuration](path: String, steps: String, appName: Option[String])(implicit cr: ConfigReader[C])
  extends BaseETLContext[String, C](path, steps, appName) {

  override val ETLType: ClassTag[String] = classTag[String]
  override val dataMinValue: String = "0"
  override val defaultDataCurrentValue: String = ""
}

/**
 * The default ETL Context is a timestamp-based ETL to capture changes using a timestamp column.
 */
case class RuntimeETLContext(
                              @arg(name = "config", short = 'c', doc = "Config path") path: String,
                              @arg(name = "steps", short = 's', doc = "Steps") steps: String,
                              @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                            ) extends TimestampETLContext[SimpleConfiguration](path, steps, appName)

case class RuntimeTimestampETLContext(
                                       @arg(name = "config", short = 'c', doc = "Config path") path: String,
                                       @arg(name = "steps", short = 's', doc = "Steps") steps: String,
                                       @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                                     ) extends TimestampETLContext[SimpleConfiguration](path, steps, appName)

case class RuntimeIdETLContext(
                                @arg(name = "config", short = 'c', doc = "Config path") path: String,
                                @arg(name = "steps", short = 's', doc = "Steps") steps: String,
                                @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                              ) extends IdETLContext[SimpleConfiguration](path, steps, appName)

object RuntimeETLContext {
  implicit def ETLConfigParser: ParserForClass[RuntimeETLContext] = ParserForClass[RuntimeETLContext]

  implicit def TimestampETLConfigParser: ParserForClass[RuntimeTimestampETLContext] = ParserForClass[RuntimeTimestampETLContext]

  implicit def IdETLConfigParser: ParserForClass[RuntimeIdETLContext] = ParserForClass[RuntimeIdETLContext]

}

