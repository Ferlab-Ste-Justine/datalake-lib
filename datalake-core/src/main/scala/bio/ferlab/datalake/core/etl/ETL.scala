package bio.ferlab.datalake.core.etl

import bio.ferlab.datalake.core.config.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * Defines a common workflow for ETL jobs.
 * By definition an ETL can take 1..n sources as input and can produce only 1 output.
 * @param destination where the output data will be written
 * @param conf application configuration
 */
abstract class ETL(val destination: DataSource)(implicit val conf: Configuration) {

  /**
   * Reads data from a file system and produce a Map[DataSource, DataFrame].
   * This method should avoid transformation and joins but can implement filters in order to make the ETL more efficient.
   * @param spark an instance of SparkSession
   * @return all the data needed to pass to the transform method and produce the desired output.
   */
  def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame]

  /**
   * Takes a Map[DataSource, DataFrame] as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data
   * @param spark an instance of SparkSession
   * @return
   */
  def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame

  /**
   * Loads the output data into a persistent storage.
   * The output destination can be any of: object store, database or flat files...
   *
   * @param data output data produced by the transform method.
   * @param spark an instance of SparkSession
   */
  def load(data: DataFrame)(implicit spark: SparkSession): Unit

  /**
   * OPTIONAL - Contains all actions needed to be done in order to make the data available to users
   * like creating a view with the data.
   * @param spark an instance of SparkSession
   */
  def publish()(implicit spark: SparkSession): Unit = {

  }

  /**
   * Entry point of the etl - execute this method in order to run the whole ETL
   * @param spark an instance of SparkSession
   */
  def run()(implicit spark: SparkSession): Unit = {
    val inputs = extract()
    val output = transform(inputs)
    load(output)
    publish()
  }

}
