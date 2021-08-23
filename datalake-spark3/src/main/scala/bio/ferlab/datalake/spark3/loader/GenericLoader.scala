package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

import java.time.LocalDate

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
                     (implicit spark:  SparkSession): DataFrame = ???

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
                   (implicit spark:  SparkSession): DataFrame = ???

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table for which keeping the full historic is not required.
   *
   * @param location      full path of where the data will be located
   * @param tableName     the name of the updated/created table
   * @param updates       new data to be merged with existing data
   * @param primaryKeys   name of the columns holding the unique id
   * @param oidName       name of the column holding the hash of the column that can change over time (or version number)
   * @param createdOnName name of the column holding the creation timestamp
   * @param updatedOnName name of the column holding the last update timestamp
   * @param spark         a valid spark session
   * @return the data as a dataframe
   */
  override def scd2(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: List[String],
                    format: String,
                    validFromName: String,
                    validToName: String,
                    minValidFromDate: LocalDate,
                    maxValidToDate: LocalDate)(implicit spark: SparkSession): DataFrame = ???
}

