package bio.ferlab.datalake.spark3.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JupyterUtils {

  val defaultConf = Map(
    "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
    "spark.hadoop.fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.path.style.access" -> "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled" -> "true",
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.delta.merge.repartitionBeforeWrite" -> "true"
  )

  def initSparkSession(conf: Map[String, String],
                       existingSparkSession: SparkSession): SparkSession = {
    val existingConf = existingSparkSession.sparkContext.getConf
    existingSparkSession.stop()

    val sparkConf: SparkConf = conf.foldLeft(existingConf){ case (c, (k, v)) => c.set(k, v) }

    SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .appName("SparkApp")
      .getOrCreate()
  }

  def initSparkSession(s3accessKey: String,
                       s3secretKey: String,
                       s3Endpoint: String,
                       existingSparkSession: SparkSession): SparkSession = {

    val conf = defaultConf ++ Map(
      "fs.s3a.access.key" -> s3accessKey,
      "fs.s3a.secret.key" -> s3secretKey,
      "spark.hadoop.fs.s3a.endpoint" -> s3Endpoint
    )
    initSparkSession(conf, existingSparkSession)
  }
}
