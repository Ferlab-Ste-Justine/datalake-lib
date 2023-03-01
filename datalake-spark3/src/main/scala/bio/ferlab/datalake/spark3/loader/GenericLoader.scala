package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql._

import java.time.LocalDate
import scala.util.{Failure, Success, Try}

object GenericLoader extends Loader {

  private def getDataFrameWriter(df: DataFrame,
                                 format: String,
                                 mode: SaveMode,
                                 partitioning: List[String],
                                 options: Map[String, String]): DataFrameWriter[Row] = {
      df
        .write
        .options(options)
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
            location: String,
            options: Map[String, String]): DataFrame = {

    val dataFrameWriter = getDataFrameWriter(df, format, mode, partitioning, options)

    tableName match {
      case "" =>
        dataFrameWriter.save(location)
      case table =>
        dataFrameWriter.option("path", location).saveAsTable(s"`$databaseName`.`$table`")
    }

    df
  }

  override def read(location: String,
                    format: String,
                    readOptions: Map[String, String],
                    databaseName: Option[String],
                    tableName: Option[String])(implicit spark: SparkSession): DataFrame = {
    Try {
      (databaseName, tableName) match {
        case (None, Some(name)) =>
          spark.table(name)
        case (Some(db), Some(name)) =>
          spark.table(s"$db.$name")
        case (_, _) =>
          spark.read.options(readOptions).format(format).load(location)
      }
    }.getOrElse(spark.read.options(readOptions).format(format).load(location))


  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: List[String],
                         format: String,
                         options: Map[String, String])
                        (implicit spark: SparkSession): DataFrame = {
    write(df, format, SaveMode.Overwrite, partitioning, databaseName, tableName, location, options)
  }

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates:  DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])
                     (implicit spark:  SparkSession): DataFrame = {
    require(primaryKeys.forall(updates.columns.contains), s"requires column [${primaryKeys.mkString(", ")}]")

    val fullName = s"$databaseName.$tableName"
    val writtenData = Try(read(location, format, options, Some(tableName), Some(databaseName))) match {
      case Failure(_) => writeOnce(location, databaseName, tableName, updates, partitioning, format, options)
      case Success(existing) =>
        val data = existing
          .join(updates, primaryKeys, "left_anti")
          .unionByName(updates)
          .persist()

        data.limit(1).count() //triggers transformations in order to write at the same location as we read data

        val result = writeOnce(location, databaseName, tableName, data, partitioning, format, options)
        if (fullName.nonEmpty) {
          spark.sql(s"REFRESH TABLE $fullName")
        }
        result
    }
    writtenData
  }

  def insert(location: String,
             databaseName: String,
             tableName: String,
             updates: DataFrame,
             partitioning: List[String],
             format: String,
             options: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    write(updates, format, SaveMode.Append, partitioning, databaseName, tableName, location, options)
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
                    format: String,
                    options: Map[String, String])
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
                    buidName: String,
                    oidName: String,
                    isCurrentName: String,
                    partitioning: List[String],
                    format: String,
                    validFromName: String,
                    validToName: String,
                    options: Map[String, String],
                    minValidFromDate: LocalDate,
                    maxValidToDate: LocalDate)(implicit spark: SparkSession): DataFrame = ???

  /**
   * Keeps old partition and overwrite new partitions.
   *
   * @param location     where to write the data
   * @param databaseName database name
   * @param tableName    table name
   * @param df           new data to write into the table
   * @param partitioning how the data is partitionned
   * @param format       format
   * @param options      write options
   * @param spark        a spark session
   * @return updated data
   */
  override def overwritePartition(location: String,
                                  databaseName: String,
                                  tableName: String,
                                  df: DataFrame,
                                  partitioning: List[String],
                                  format: String,
                                  options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

}

