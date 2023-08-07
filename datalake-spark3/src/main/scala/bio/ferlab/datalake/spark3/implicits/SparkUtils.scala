package bio.ferlab.datalake.spark3.implicits

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.net.URI
import scala.annotation.tailrec


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
    s"$database.${table}_${studyId.toLowerCase}_${releaseId.toLowerCase}"
  }

  def colFromArrayOrField(df: DataFrame, colName: String): Column = {
    df.schema(colName).dataType match {
      case ArrayType(_, _) => df(colName)(0)
      case _ => df(colName)
    }
  }

  def union(df1: DataFrame, df2: DataFrame)(implicit spark: SparkSession): DataFrame = (df1, df2) match {
    case (p, c) if p.isEmpty => c
    case (p, c) if c.isEmpty => p
    case (p, c) => p.union(c)
    case _ => spark.emptyDataFrame
  }

  def firstAs(c: String, ignoreNulls: Boolean = false): Column = first(col(c), ignoreNulls) as c

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

  def array_remove_empty(array: Column): Column = {
    //array_reoe null does not work
    array_remove(filter(array, a => a.isNotNull), lit(""))
  }

  def getColumnOrElseArray(colName: String, default: Any = ""): Column = when(col(colName).isNull, array(lit(default))).otherwise(transform(col(colName), c => trim(c)))

  def isNestedFieldExists(df: DataFrame, fieldName: String): Boolean = isNestedFieldExists(df.schema, fieldName)

  def isNestedFieldExists(dfSchema: StructType, fieldName: String): Boolean = {
    @tailrec
    def findField(schema: StructType, nameParts: List[String]): Option[DataType] = {
      nameParts match {
        case Nil => None
        case head :: Nil => schema.find(_.name == head).map(_.dataType)
        case head :: tail => schema.find(_.name == head) match {
          case Some(field) => field.dataType match {
            case structType: StructType => findField(structType, tail)
            case _ => None
          }
          case None => None
        }
      }
    }

    val nameParts = fieldName.split('.').toList
    findField(dfSchema, nameParts).isDefined
  }

  implicit class SparkOperations(df: DataFrame) {
    /**
     * Converts a DataFrame to RDD, caches it then converts it back to DataFrame.
     *
     * It is a workaround to solve slow query planning, for example in iterative algorithms. It truncates the lineage
     * of the DataFrame by preventing the SQL optimizer (Catalyst) to look past the RDD. Checkpointing has a similar
     * result but it saves the data to files inside the checkpoint directory without deleting them after the job.
     * @param spark Instance of SparkSession
     * @return The same DataFrame
     * @see [[https://medium.com/@hareesha1906/apache-spark-large-query-plans-54f3f3c9c7d3]]
     */
    def cacheRDD()(implicit spark: SparkSession): DataFrame = {
      val rdd = df.rdd
      rdd.cache()
      spark.createDataFrame(rdd, df.schema)
    }
  }
}
