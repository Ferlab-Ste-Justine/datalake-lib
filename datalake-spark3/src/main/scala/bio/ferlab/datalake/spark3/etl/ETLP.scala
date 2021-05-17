package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}
import bio.ferlab.datalake.spark3.hive.UpdateTableComments
import org.apache.spark.sql.functions.{col, lit, regexp_extract, trim}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

abstract class ETLP()(implicit conf: Configuration) extends ETL {

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }

  override def publish()(implicit spark: SparkSession): Unit = {
    UpdateTableComments.run(destination.database, destination.name, destination.documentationpath)
    Try { spark.sql(s"drop table if exists ${destination.view}.${destination.name}") }
    spark.sql(s"create or replace view ${destination.view}.${destination.name} as select * from ${destination.database}.${destination.name}")
  }

  private def regexp_extractFromCreateStatement[T](regex: String, defaultValue: T)(implicit spark: SparkSession): T = {
    Try {
      spark.sql(s"show create table ${destination.database}.${destination.name}")
        .withColumn("extracted_value", regexp_extract(col("createtab_stmt"), regex, 1))
        .where(trim(col("extracted_value")) =!= lit(""))
        .select("extracted_value")
        .collect().head.getAs[T](0)
    }.getOrElse(defaultValue)
  }

  def lastReleaseId(implicit spark: SparkSession): String =
    regexp_extractFromCreateStatement("(re_\\d{6})", "re_000001")
}


