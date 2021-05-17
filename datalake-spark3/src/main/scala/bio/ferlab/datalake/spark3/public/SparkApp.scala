package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends App {

  def init(): (Configuration, SparkSession) = {

    println(s"LOADING ${args(0)}...")

    val conf: Configuration = ConfigurationLoader.loadFromResources(args(0))

    val sparkConf: SparkConf = conf.sparkconf.foldLeft(new SparkConf()){ case (c, (k, v)) => c.set(k, v) }

    val spark: SparkSession =
      SparkSession
        .builder
        .config(sparkConf)
        .enableHiveSupport()
        .appName("SparkApp")
        .getOrCreate()
    (conf, spark)
  }

}
