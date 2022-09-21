package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.LoadType.{Scd1, Scd2}
import bio.ferlab.datalake.commons.config.WriteOptions.{UPDATED_ON_COLUMN_NAME, VALID_FROM_COLUMN_NAME}
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, RunStep}
import bio.ferlab.datalake.spark3.datastore.SqlBinderResolver
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import scala.util.Try


/**
 * @deprecated
 * use [[ETLSingleDestination]] instead
 *
 * Defines a common workflow for ETL jobs.
 * By definition an ETL can take 1..n sources as input and can produce only 1 output.
 * @param conf application configuration
 */
@deprecated("use [[v2.ETL]] instead", "0.2.0")
abstract class ETL()(implicit val conf: Configuration) {

  val minDateTime: LocalDateTime = LocalDateTime.of(1900, 1, 1, 0, 0, 0)
  val maxDateTime: LocalDateTime = LocalDateTime.of(9999, 12, 31, 23, 59, 55)
  val destination: DatasetConf

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

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
    log.info(s"LOADING: ${destination.id}")
    if(LoadResolver.write(spark, conf).isDefinedAt(destination.format -> destination.loadtype)) {
      Try(
        destination.table.foreach(table => spark.sql(s"CREATE DATABASE IF NOT EXISTS ${table.database}"))
      )
      LoadResolver
        .write(spark, conf)(destination.format -> destination.loadtype)
        .apply(destination, data)
    } else {
      throw new NotImplementedError(s"Load is not implemented for [${destination.format} / ${destination.loadtype}]")
    }
  }

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
  def run(runSteps: Seq[RunStep] = RunStep.default_load,
          lastRunDateTime: Option[LocalDateTime] = None,
          currentRunDateTime: Option[LocalDateTime] = None)(implicit spark: SparkSession): DataFrame = {

    val lastRunDate = lastRunDateTime.getOrElse(if (runSteps.contains(RunStep.reset)) minDateTime else getLastRunDateFor(destination))
    val currentRunDate = currentRunDateTime.getOrElse(LocalDateTime.now())

    if (runSteps.isEmpty)
      log.info(s"WARNING ETL started with no runSteps. Nothing will be executed.")
    else
      log.info(s"RUN steps: \t\t ${runSteps.mkString(" -> ")}")

    log.info(s"RUN lastRunDate: \t $lastRunDate")
    log.info(s"RUN currentRunDate: \t $currentRunDate")

    if (runSteps.contains(RunStep.reset)) this.reset()

    val data: Map[String, DataFrame] =
      if (runSteps.contains(RunStep.extract) && runSteps.contains(RunStep.sample)) {
        extract(lastRunDate, currentRunDate).map { case (ds, df) => ds -> sampling(ds).apply(df) }
      } else if (runSteps.contains(RunStep.extract)) {
        extract(lastRunDate, currentRunDate)
      } else {
        Map()
      }

    val output: DataFrame =
      if (runSteps.contains(RunStep.transform)) {
        transform(data, lastRunDate, currentRunDate)
      } else {
        spark.emptyDataFrame
      }

    if (runSteps.contains(RunStep.load)) {
      load(output)
    } else {
      output.show(false) //triggers extract and transform method in case load method is not called
    }

    if (runSteps.contains(RunStep.publish)) {
      publish()
    }
    Try(destination.read).getOrElse(spark.emptyDataFrame)
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
          ds.read.select(max(col(ds.writeoptions(UPDATED_ON_COLUMN_NAME)))).limit(1).as[Timestamp].head().toLocalDateTime
        ).getOrElse(minDateTime)

      case Scd2 =>
        Try(
          ds.read.select(max(col(ds.writeoptions(VALID_FROM_COLUMN_NAME)))).limit(1).as[Date].head().toLocalDate.atStartOfDay()
        ).getOrElse(minDateTime)

      case _ => minDateTime

    }
  }

  /**
   * Reset the ETL by removing the destination dataset.
   */
  def reset()(implicit spark: SparkSession): Unit = {
    FileSystemResolver
      .resolve(conf.getStorage(destination.storageid).filesystem)
      .remove(destination.path)

    SqlBinderResolver.drop(spark, conf)(destination.format).apply(destination)

  }

  /**
   * Logic used when the ETL is run as a [[SAMPLE_LOAD]]
   * @return
   */
  def sampling: PartialFunction[String, DataFrame => DataFrame] = defaultSampling

  def defaultSampling: PartialFunction[String, DataFrame => DataFrame] = {
    case _ => df => df.sample(0.05)
  }

}
