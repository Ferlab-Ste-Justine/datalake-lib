package bio.ferlab.datalake.spark3.hive

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.datastore.HiveSqlBinder
import org.apache.spark.sql.SparkSession

import scala.util.Try

object UpdateTableComments {

  def run(ds: DatasetConf)(implicit spark: SparkSession): Unit = {
    if(ds.table.nonEmpty)
      run(ds.table.get.database, ds.table.get.name, ds.documentationpath.get)
  }

  def run(database: String, table: String, metadata_file: String)(implicit spark: SparkSession): Unit = {
    Try {
      spark.read.option("multiline", "true").json(metadata_file).drop("data_type")
    }.fold(_ => println(s"[ERROR] documentation ${metadata_file} not found."),
      documentationDf => {
        import spark.implicits._
        val describeTableDF = spark.sql(s"DESCRIBE $database.$table")
        val comments = describeTableDF.drop("comment").join(documentationDf, Seq("col_name"))
          .as[HiveFieldComment].collect()

        setComments(comments, database, table)
      }
    )
  }

  def setComments(comments: Array[HiveFieldComment], database: String, table: String)(implicit spark: SparkSession): Unit = {
    comments.foreach {
      case HiveFieldComment(name, _type, comment) => HiveSqlBinder.setComment(name, _type, comment, database, table)
    }
  }

  def clearComments(database: String, table: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val emptyComments = spark.sql(s"DESCRIBE $database.$table").as[HiveFieldComment].collect().map(_.copy(comment = ""))
    setComments(emptyComments, database, table)
  }

}

