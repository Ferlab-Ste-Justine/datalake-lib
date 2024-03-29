package bio.ferlab.datalake.commons.config

import mainargs.{ParserForClass, TokensReader, arg}
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

abstract class BaseETLContext[T, C <: Configuration](path: String, steps: Seq[RunStep], appName: Option[String])(implicit cr: ConfigReader[C])
  extends ETLContext[T, C] {
  private val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)
  log.info(s"Loading config file: [$path]")

  lazy val config: C = ConfigurationLoader.loadFromResources[C](path)

  lazy val sparkConf: SparkConf = this.config.sparkconf.foldLeft(new SparkConf()) { case (c, (k, v)) => c.set(k, v) }

  lazy val runSteps: Seq[RunStep] = steps

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
abstract class TimestampETLContext[C <: Configuration](path: String, steps: Seq[RunStep], appName: Option[String])(implicit cr: ConfigReader[C])
  extends BaseETLContext[LocalDateTime, C](path, steps, appName) {

  override val ETLType: ClassTag[LocalDateTime] = classTag[LocalDateTime]
  override val dataMinValue: LocalDateTime = LocalDateTime.of(1900, 1, 1, 0, 0, 0)

  def defaultDataCurrentValue: LocalDateTime = LocalDateTime.now() // Use def to evaluate on call
}

/**
 * Context for ID-based ETL.
 */
abstract class IdETLContext[C <: Configuration](path: String, steps: Seq[RunStep], appName: Option[String])(implicit cr: ConfigReader[C])
  extends BaseETLContext[String, C](path, steps, appName) {

  override val ETLType: ClassTag[String] = classTag[String]
  override val dataMinValue: String = "0"
  override val defaultDataCurrentValue: String = ""
}

abstract class ContextParser[T] {
  implicit def contextParser: ParserForClass[T]
}

trait StepsParser {
  implicit object StepsParser extends TokensReader.Simple[Seq[RunStep]] {
    override val shortName: String = "steps"

    override def read(strs: Seq[String]): Either[String, Seq[RunStep]] = Right(RunStep.getSteps(strs.head))
  }
}

/**
 * The default ETL Context is a timestamp-based ETL to capture changes using a timestamp column.
 */
case class RuntimeETLContext(
                              @arg(name = "config", short = 'c', doc = "Config path") path: String,
                              @arg(name = "steps", short = 's', doc = "Steps") steps: Seq[RunStep],
                              @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                            ) extends TimestampETLContext[SimpleConfiguration](path, steps, appName)

object RuntimeETLContext extends ContextParser[RuntimeETLContext] with StepsParser {
  implicit def contextParser: ParserForClass[RuntimeETLContext] = ParserForClass[RuntimeETLContext]
}

case class RuntimeTimestampETLContext(
                                       @arg(name = "config", short = 'c', doc = "Config path") path: String,
                                       @arg(name = "steps", short = 's', doc = "Steps") steps: Seq[RunStep],
                                       @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                                     ) extends TimestampETLContext[SimpleConfiguration](path, steps, appName)

object RuntimeTimestampETLContext extends ContextParser[RuntimeTimestampETLContext] with StepsParser {
  implicit def contextParser: ParserForClass[RuntimeTimestampETLContext] = ParserForClass[RuntimeTimestampETLContext]
}

case class RuntimeIdETLContext(
                                @arg(name = "config", short = 'c', doc = "Config path") path: String,
                                @arg(name = "steps", short = 's', doc = "Steps") steps: Seq[RunStep],
                                @arg(name = "app-name", short = 'a', doc = "App name") appName: Option[String]
                              ) extends IdETLContext[SimpleConfiguration](path, steps, appName)

object RuntimeIdETLContext extends ContextParser[RuntimeIdETLContext] with StepsParser {
  implicit def contextParser: ParserForClass[RuntimeIdETLContext] = ParserForClass[RuntimeIdETLContext]
}
