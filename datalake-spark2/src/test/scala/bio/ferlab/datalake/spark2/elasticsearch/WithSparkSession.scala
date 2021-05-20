package bio.ferlab.datalake.spark2.elasticsearch

import org.apache.spark.sql.SparkSession

trait WithSparkSession {
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .enableHiveSupport()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
}
