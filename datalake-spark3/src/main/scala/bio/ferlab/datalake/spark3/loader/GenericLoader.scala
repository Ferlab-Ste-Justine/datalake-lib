package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

object GenericLoader extends Loader {

  private def getDataFrameWriter(df: DataFrame, format: String, mode: SaveMode, partitioning: List[String]): DataFrameWriter[Row] = {
      df
        .write
        .format(format)
        .mode(mode)
        .partitionBy(partitioning:_*)
  }

  def write(df: DataFrame,
            format: String,
            mode: SaveMode,
            partitioning: List[String],
            databaseName: String,
            tableName: String,
            location: String): DataFrame = {
    val dataFrameWriter = getDataFrameWriter(df, format, mode, partitioning)

    tableName match {
      case "" =>
        dataFrameWriter.save(location)
      case table =>
        dataFrameWriter.option("path", location).saveAsTable(s"$databaseName.$table")
    }

    df
  }

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
    write(df, format, SaveMode.Overwrite, partitioning, databaseName, tableName, location)
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

  def insert(location: String,
             databaseName: String,
             tableName: String,
             updates: DataFrame,
             partitioning: List[String],
             format: String)(implicit spark: SparkSession): DataFrame = {
    write(updates, format, SaveMode.Append, partitioning, databaseName, tableName, location)
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

