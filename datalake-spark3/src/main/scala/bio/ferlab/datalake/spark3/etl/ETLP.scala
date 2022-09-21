package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.hive.UpdateTableComments
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, regexp_extract, trim}

import scala.util.Try


abstract class ETLP()(implicit conf: Configuration) extends ETLSingleDestination {

  override def publish()(implicit spark: SparkSession): Unit = {

    if (mainDestination.documentationpath.nonEmpty && mainDestination.table.nonEmpty) {
      val t = mainDestination.table.get
      UpdateTableComments.run(t.database, t.name, mainDestination.documentationpath.get)
    }

    if (mainDestination.view.nonEmpty && mainDestination.table.nonEmpty) {
      val v = mainDestination.view.get
      val t = mainDestination.table.get

      Try { spark.sql(s"drop table if exists ${v.fullName}") }
      spark.sql(s"create or replace view ${v.fullName} as select * from ${t.fullName}")

    }
  }

  private def regexp_extractFromCreateStatement[T](regex: String, defaultValue: T)(implicit spark: SparkSession): T = {
    Try {
      val table = mainDestination.table.get
      spark.sql(s"show create table ${table.fullName}")
        .withColumn("extracted_value", regexp_extract(col("createtab_stmt"), regex, 1))
        .where(trim(col("extracted_value")) =!= lit(""))
        .select("extracted_value")
        .collect().head.getAs[T](0)
    }.getOrElse(defaultValue)
  }

  def lastReleaseId(implicit spark: SparkSession): String =
    regexp_extractFromCreateStatement("(re_\\d{6})", "re_000001")
}


