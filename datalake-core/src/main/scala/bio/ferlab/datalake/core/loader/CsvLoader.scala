package bio.ferlab.datalake.core.loader

import bio.ferlab.datalake.core.etl.Partitioning
import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvLoader extends Loader {

  /**
   * Default read logic for a loader
   * @param location absolute path of where the data is
   * @param format string representing the format
   * @param readOptions read options
   * @param spark spark session
   * @return the data as a dataframe
   */
  override def read(location: String, format: String, readOptions: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    spark.read.options(readOptions).csv(location)
  }

  /**
   * Overwrites the data located in output/tableName
   * usually used for small/test tables.
   *
   * @param df                   the data to write
   * @param tableName            the name of the table
   * @param location             full path of where the data will be located
   * @param dataChange           if the data is expected to be different from the data already written
   * @param spark                a valid spark session
   * @return the data as a dataframe
   */
  override def writeOnce(location: String,
                         databaseName: String,
                         tableName:  String,
                         df:  DataFrame,
                         partitioning: Partitioning,
                         dataChange: Boolean)
                        (implicit spark:  SparkSession): DataFrame = {
    throw new NotImplementedError("bio.ferlab.datalake.core.loader.Csv writeOnce() method is not implemented")
  }

  /**
   * Update or insert data into a table
   * usually used for fact table where duplicates need to be resolved as one record.
   *
   * @param location             full path of where the data will be located
   * @param tableName            the name of the updated/created table
   * @param updates              new data to be merged with existing data
   * @param uidName              name of the column holding the unique id
   * @param spark                a valid spark session
   * @return the data as a dataframe
   */
override def upsert(location: String,
                    databaseName: String,
                    tableName: String,
                    updates:  DataFrame,
                    uidName: String,
                    partitioning: Partitioning)
                   (implicit spark:  SparkSession): DataFrame = {
  throw new NotImplementedError("bio.ferlab.datalake.core.loader.Csv writeOnce() method is not implemented")
}

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table where keeping the full historic of the data is not important.
   *
   * @param location             full path of where the data will be located
   * @param tableName            the name of the updated/created table
   * @param updates              new data to be merged with existing data
   * @param uidName              name of the column holding the unique id
   * @param oidName              name of the column holding the hash of the column that can change over time (or version number)
   * @param createdOnName        name of the column holding the creation timestamp
   * @param updatedOnName        name of the column holding the last update timestamp
   * @param spark                a valid spark session
   * @return the data as a dataframe
   */
  override def scd1(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    uidName: String,
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: Partitioning)
                   (implicit spark:  SparkSession): DataFrame = {
    throw new NotImplementedError("bio.ferlab.datalake.core.loader.Csv writeOnce() method is not implemented")
  }
}
