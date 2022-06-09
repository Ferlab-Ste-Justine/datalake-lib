package bio.ferlab.datalake.spark3.datastore

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {

  /**
   * Default read logic for a loader
   * @param location absolute path of where the data is
   * @param format string representing the format
   * @param readOptions read options
   * @param databaseName Optional database name
   * @param tableName Optional table name
   * @param spark spark session
   * @return the data as a dataframe
   */
  def read(location: String,
           format: String,
           readOptions: Map[String, String],
           databaseName: Option[String],
           tableName: Option[String])(implicit spark: SparkSession): DataFrame

  /**
   * Default read logic for a specific date.
   * @param location absolute path of where the data is
   * @param format string representing the format
   * @param readOptions read options
   * @param databaseName Optional database name
   * @param tableName Optional table name
   * @param spark spark session
   * @return the data as a dataframe
   */
  def readForDates(location: String,
                   format: String,
                   readOptions: Map[String, String],
                   databaseName: Option[String],
                   tableName: Option[String],
                   fromColumnName: String,
                   fromValue: Any,
                   beforeColumnName: String,
                   beforeValue: Any)(implicit spark: SparkSession): DataFrame

}
