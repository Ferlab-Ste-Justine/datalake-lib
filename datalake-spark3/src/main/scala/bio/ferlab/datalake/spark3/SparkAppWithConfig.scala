package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, RunStep}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j
import pureconfig.ConfigReader
abstract class SparkAppWithConfig[T <: Configuration](implicit cr:ConfigReader[T]) extends App {

  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def init(appName: String = "SparkApp"): (T, Seq[RunStep], SparkSession) = init(args(0), args(1), appName)

  def init(configurationPath: String, runSteps: String, appName: String): (T, Seq[RunStep], SparkSession) = {

    log.info(s"Loading config file: [$configurationPath]")

    val conf: T = ConfigurationLoader.loadFromResources[T](configurationPath)

    val sparkConf: SparkConf = conf.sparkconf.foldLeft(new SparkConf()) { case (c, (k, v)) => c.set(k, v) }

    val rt = RunStep.getSteps(runSteps)

    val spark: SparkSession =
      SparkSession
        .builder
        .config(sparkConf)
        .enableHiveSupport()
        .appName(appName)
        .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    (conf, rt, spark)
  }
}
