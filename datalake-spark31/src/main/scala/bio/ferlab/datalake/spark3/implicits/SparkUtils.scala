package bio.ferlab.datalake.spark3.implicits

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.net.URI


object SparkUtils {

  val filename: Column = regexp_extract(input_file_name(), ".*/(.*)", 1)

  /** Check if the hadoop file exists
   *
   * @param path  Path to check. Accept some patterns
   * @param spark session that contains hadoop config
   * @return
   */
  def fileExist(path: String)(implicit spark: SparkSession): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = if (path.startsWith("s3a")) {
      val bucket = path.replace("s3a://", "").split("/").head
      org.apache.hadoop.fs.FileSystem.get(new URI(s"s3a://$bucket"), conf)
    } else {
      org.apache.hadoop.fs.FileSystem.get(conf)
    }

    val statuses = fs.globStatus(new Path(path))
    statuses != null && statuses.nonEmpty
  }

  def tableName(table: String, studyId: String, releaseId: String): String = {
    s"${table}_${studyId.toLowerCase}_${releaseId.toLowerCase}"
  }

  def tableName(table: String,
                studyId: String,
                releaseId: String,
                database: String = "variant"): String = {
    s"${database}.${table}_${studyId.toLowerCase}_${releaseId.toLowerCase}"
  }

  def colFromArrayOrField(df: DataFrame, colName: String): Column = {
    df.schema(colName).dataType match {
      case ArrayType(_, _) => df(colName)(0)
      case _               => df(colName)
    }
  }

  def union(df1: DataFrame, df2: DataFrame)(implicit spark: SparkSession): DataFrame = (df1, df2) match {
    case (p, c) if p.isEmpty => c
    case (p, c) if c.isEmpty => p
    case (p, c)              => p.union(c)
    case _                   => spark.emptyDataFrame
  }

  def firstAs(c: String): Column = first(col(c)) as c

  def escapeInfoAndLowercase(df: DataFrame, excludes: String*): Seq[Column] = {
    df.columns.collect {
      case c if c.startsWith("INFO") && !excludes.contains(c) =>
        col(c) as c.replace("INFO_", "").toLowerCase
    }
  }

  val removeEmptyObjectsIn: String => Column = column =>
    when(to_json(col(column)) === lit("[{}]"), array()).otherwise(col(column))

  def getColumnOrElse(colName: String, default: Any = ""): Column =
    when(col(colName).isNull, lit(default)).otherwise(trim(col(colName)))

}