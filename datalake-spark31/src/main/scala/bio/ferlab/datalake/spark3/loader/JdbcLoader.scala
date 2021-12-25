package bio.ferlab.datalake.spark3.loader
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate

object JdbcLoader extends Loader {

  /**
   * Default read logic for a loader
   * @param location     absolute path of where the data is
   * @param format       string representing the format
   * @param readOptions  read options
   * @param databaseName Optional database name
   * @param tableName    Optional table name
   * @param spark        spark session
   * @return the data as a dataframe
   */
  override def read(location: String,
                    format: String,
                    readOptions: Map[String, String],
                    databaseName: Option[String],
                    tableName: Option[String])(implicit spark: SparkSession): DataFrame = {
    require(readOptions.isDefinedAt("url"), "Expecting [url] to be defined in readOptions.")
    require(readOptions.isDefinedAt("user"), "Expecting [user] to be defined in readOptions.")
    require(readOptions.isDefinedAt("password"), "Expecting [password] to be defined in readOptions.")
    require(readOptions.isDefinedAt("driver"), "Expecting [driver] to be defined in readOptions.")
    require(readOptions.isDefinedAt("query") || readOptions.isDefinedAt("dbtable"),
      "Expecting either [query] or [dbtable] to be defined in readOptions.")

    spark
      .read
      .format(format)
      .options(readOptions)
      .load()

  }

  /**
   * @param location where to write the data
   * @param databaseName database name
   * @param tableName table name
   * @param df new data to write into the table
   * @param partitioning how the data is partitionned
   * @param format format
   * @param options write options
   * @param spark a spark session
   *  @return updated data
   */
  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: List[String],
                         format: String,
                         options: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    df
      .write
      .format(format)
      .mode(SaveMode.Overwrite)
      .options(options + ("dbtable" -> s"$databaseName.$tableName"))
      .save()
    df
  }

  /**
   * Insert or append data into a table
   * Does not resolve duplicates
   *
   * @param location  full path of where the data will be located
   * @param tableName the name of the updated/created table
   * @param updates   new data to be merged with existing data
   * @param spark     a valid spark session
   * @return the data as a dataframe
   */
  override def insert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    updates
      .write
      .format(format)
      .mode(SaveMode.Append)
      .options(options + ("dbtable" -> s"$databaseName.$tableName"))
      .save()
    updates
  }

  /**
   * Update or insert data into a table
   * Resolves duplicates by using the list of primary key passed as argument
   * @param location full path of where the data will be located
   * @param tableName the name of the updated/created table
   * @param updates new data to be merged with existing data
   * @param primaryKeys name of the columns holding the unique id
   * @param spark a valid spark session
   * @return the data as a dataframe
   */
  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

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
  override def scd1(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: List[String],
                    format: String)(implicit spark: SparkSession): DataFrame = ???

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
