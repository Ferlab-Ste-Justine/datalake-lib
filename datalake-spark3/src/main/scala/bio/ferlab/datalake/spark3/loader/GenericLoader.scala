package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql.{DataFrame, SparkSession}

object GenericLoader extends Loader {

  override def read(location: String, format: String, readOptions: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    spark.read.options(readOptions).format(format).load(location)
  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: List[String],
                         format: String,
                         dataChange: Boolean)
                        (implicit spark: SparkSession): DataFrame = {
    val dataFrameWriter =
      df
        .write
        .format(format)
        .mode("overwrite")
        .partitionBy(partitioning:_*)

    tableName match {
      case "" =>
        dataFrameWriter.save(location)
      case table =>
        dataFrameWriter.option("path", location).saveAsTable(s"$databaseName.$table")
    }

    df
  }

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates:  DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String],
                      format: String)
                     (implicit spark:  SparkSession): DataFrame = {
    throw NotImplementedException
  }

  override def scd1(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: List[String],
                    format: String)
                   (implicit spark:  SparkSession): DataFrame = {
    throw NotImplementedException
  }
}

