package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.file.{FileSystem, HadoopFileSystem}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.loader.LoadType.{Scd1, Scd2}
import bio.ferlab.datalake.spark3.loader.WriteOptions.{UPDATED_ON_COLUMN_NAME, VALID_FROM_COLUMN_NAME}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import scala.util.Try


/**
 * Defines a common workflow for ETL jobs.
 * By definition an ETL can take 1..n sources as input and can produce only 1 output.
 * @param conf application configuration
 */
abstract class ETL()(implicit val conf: Configuration) {

  val minDateTime: LocalDateTime = LocalDateTime.of(1900, 1, 1, 0, 0, 0)
  val maxDateTime: LocalDateTime = LocalDateTime.of(9999, 12, 31, 23, 59, 55)
  val destination: DatasetConf

  /**
   * Default file system
   */
  val fs: FileSystem = HadoopFileSystem

  /**
   * Reads data from a file system and produce a Map[DatasetConf, DataFrame].
   * This method should avoid transformation and joins but can implement filters in order to make the ETL more efficient.
   * @param spark an instance of SparkSession
   * @return all the data needed to pass to the transform method and produce the desired output.
   */
  def extract(lastRunDateTime: LocalDateTime = minDateTime,
              currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame]

  /**
   * Takes a Map[DatasetConf, DataFrame] as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data input data
   * @param spark an instance of SparkSession
   * @return
   */
  def transform(data: Map[String, DataFrame],
                lastRunDateTime: LocalDateTime = minDateTime,
                currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame

  /**
   * Loads the output data into a persistent storage.
   * The output destination can be any of: object store, database or flat files...
   *
   * @param data output data produced by the transform method.
   * @param spark an instance of SparkSession
   */
  def load(data: DataFrame,
           lastRunDateTime: LocalDateTime = minDateTime,
           currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    if(LoadResolver.resolve(spark, conf).isDefinedAt(destination.format -> destination.loadtype)) {
      LoadResolver
        .resolve(spark, conf)(destination.format -> destination.loadtype)
        .apply(destination, data)
    } else {
      throw new NotImplementedError(s"Load is not implemented for [${destination.format} / ${destination.loadtype}]")
    }
    data
  }

  /**
   * OPTIONAL - Contains all actions needed to be done in order to make the data available to users
   * like creating a view with the data.
   * @param spark an instance of SparkSession
   */
  def publish()(implicit spark: SparkSession): Unit = {

  }

  /**
   * Entry point of the etl - execute this method in order to run the whole ETL for a specific date
   * @param lastRunDateTime the last time this etl was run. default is [[minDateTime]]
   * @param currentRunDateTime the time at which the etl needs to be ran, usually now().
   * @param spark an instance of SparkSession
   */
  def run(lastRunDateTime: LocalDateTime = minDateTime,
          currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val inputs = extract(lastRunDateTime, currentRunDateTime)
    val output = transform(inputs, lastRunDateTime, currentRunDateTime)
    val finalDf = load(output, lastRunDateTime, currentRunDateTime)
    publish()
    finalDf
  }

  /**
   * Entry point of the etl - execute this method in order to run the whole ETL
   * @param spark an instance of SparkSession
   */
  def run()(implicit spark: SparkSession): DataFrame = {
    val lastRunDateTime = getLastRunDateFor(destination)
    val currentRunDateTime = LocalDateTime.now()
    run(lastRunDateTime, currentRunDateTime)
  }

  /**
   * If possible, fetch the last run date time from the dataset passed in argument
   * @param ds dataset
   * @param spark a spark session
   * @return the last run date or the [[minDateTime]]
   */
  def getLastRunDateFor(ds: DatasetConf)(implicit spark: SparkSession): LocalDateTime = {
    import spark.implicits._
    ds.loadtype match {
      case Scd1 =>
        Try(
          ds.read.select(max(col(ds.writeoptions(UPDATED_ON_COLUMN_NAME.value)))).limit(1).as[Timestamp].head().toLocalDateTime
        ).getOrElse(minDateTime)

      case Scd2 =>
        Try(
          ds.read.select(max(col(ds.writeoptions(VALID_FROM_COLUMN_NAME.value)))).limit(1).as[Date].head().toLocalDate.atStartOfDay()
        ).getOrElse(minDateTime)

      case _ => minDateTime

    }
  }

  /**
   * Reset the ETL by removing the destination dataset.
   */
  def reset(): Unit = {
    fs.remove(destination.location)
  }

}
