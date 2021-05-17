package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}
import bio.ferlab.datalake.spark3.file.{FileSystem, HadoopFileSystem}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * Defines a common workflow for ETL jobs.
 * By definition an ETL can take 1..n sources as input and can produce only 1 output.
 * @param conf application configuration
 */
abstract class ETL()(implicit val conf: Configuration) {

  val destination: SourceConf

  /**
   * Default file system
   */
  val fs: FileSystem = HadoopFileSystem

  /**
   * Reads data from a file system and produce a Map[DataSource, DataFrame].
   * This method should avoid transformation and joins but can implement filters in order to make the ETL more efficient.
   * @param spark an instance of SparkSession
   * @return all the data needed to pass to the transform method and produce the desired output.
   */
  def extract()(implicit spark: SparkSession): Map[SourceConf, DataFrame]

  /**
   * Takes a Map[DataSource, DataFrame] as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data
   * @param spark an instance of SparkSession
   * @return
   */
  def transform(data: Map[SourceConf, DataFrame])(implicit spark: SparkSession): DataFrame

  /**
   * Loads the output data into a persistent storage.
   * The output destination can be any of: object store, database or flat files...
   *
   * @param data output data produced by the transform method.
   * @param spark an instance of SparkSession
   */
  def load(data: DataFrame)(implicit spark: SparkSession): DataFrame

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
  def run()(implicit spark: SparkSession): DataFrame = {
    val inputs = extract()
    val output = transform(inputs)
    val finalDf = load(output)
    publish()
    finalDf
  }

}
