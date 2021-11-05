package bio.ferlab.datalake.spark3.datastore

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

trait Writer {

  /**
   * Overwrites the data located in output/tableName
   * usually used for small/test tables.
   * @param location full path of where the data will be located
   * @param databaseName the name of the database
   * @param tableName the name of the table
   * @param df the data to write
   * @param partitioning list of column used as partition
   * @param format format of the data to be written
   * @param dataChange if the data is expected to be different from the data already written
   * @param spark a valid spark session
   * @return the data as a dataframe
   */
  def writeOnce(location: String,
                databaseName: String,
                tableName: String,
                df: DataFrame,
                partitioning: List[String],
                format: String,
                dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame

  /**
   * Insert or append data into a table
   * Does not resolve duplicates
   * @param location full path of where the data will be located
   * @param tableName the name of the updated/created table
   * @param updates new data to be merged with existing data
   * @param spark a valid spark session
   * @return the data as a dataframe
   */
  def insert(location: String,
             databaseName: String,
             tableName: String,
             updates: DataFrame,
             partitioning: List[String],
             format: String)(implicit spark: SparkSession): DataFrame

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
  def upsert(location: String,
             databaseName: String,
             tableName: String,
             updates: DataFrame,
             primaryKeys: Seq[String],
             partitioning: List[String],
             format: String)(implicit spark: SparkSession): DataFrame

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table for which keeping the full historic is not required.
   *
   * @param location full path of where the data will be located
   * @param tableName the name of the updated/created table
   * @param updates new data to be merged with existing data
   * @param primaryKeys name of the columns holding the unique id
   * @param oidName name of the column holding the hash of the column that can change over time (or version number)
   * @param createdOnName name of the column holding the creation timestamp
   * @param updatedOnName name of the column holding the last update timestamp
   * @param spark a valid spark session
   * @return the data as a dataframe
   */
  def scd1(location: String,
           databaseName: String,
           tableName: String,
           updates: DataFrame,
           primaryKeys: Seq[String],
           oidName: String,
           createdOnName: String,
           updatedOnName: String,
           partitioning: List[String],
           format: String)(implicit spark: SparkSession): DataFrame

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table for which keeping the full historic is required.
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
  def scd2(location: String,
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
           maxValidToDate: LocalDate)(implicit spark: SparkSession): DataFrame

}
