package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, RunStep}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends App {

  def init(): (Configuration, Seq[RunStep], SparkSession) = init(args(0), args(1))

  def init(configurationPath: String, runSteps: String): (Configuration, Seq[RunStep], SparkSession) = {

    println(s"Loading config file: [${configurationPath}]")

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

    spark.sparkContext.setLogLevel("ERROR")

    (conf, rt, spark)
  }

}
