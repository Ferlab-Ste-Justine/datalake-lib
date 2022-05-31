package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, RunStep}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.slf4j

trait SparkApp extends App {

  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def init(): (Configuration, Seq[RunStep], SparkSession) = init(args(0), args(1))

  def init(configurationPath: String, runSteps: String): (Configuration, Seq[RunStep], SparkSession) = {

    log.info(s"Loading config file: [${configurationPath}]")

    val conf: Configuration = ConfigurationLoader.loadFromResources(configurationPath)

    val sparkConf: SparkConf = conf.sparkconf.foldLeft(new SparkConf()){ case (c, (k, v)) => c.set(k, v) }

    val rt = RunStep.getSteps(runSteps)

    val spark: SparkSession =
      SparkSession
        .builder
        .config(sparkConf)
        .enableHiveSupport()
        .appName("SparkApp")
        .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    (conf, rt, spark)
  }

}
