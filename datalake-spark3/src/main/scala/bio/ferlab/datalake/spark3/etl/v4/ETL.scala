package bio.ferlab.datalake.spark3.etl.v4

import bio.ferlab.datalake.commons.config.LoadType.{Insert, OverWritePartition, Scd1, Scd2}
import bio.ferlab.datalake.commons.config.WriteOptions.{UPDATED_ON_COLUMN_NAME, VALID_FROM_COLUMN_NAME}
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, ETLContext, IdentityRepartition, RunStep}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.datastore.SqlBinderResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import scala.reflect.classTag
import scala.util.Try


/**
 * Defines a common workflow for ETL jobs.
 * By definition an ETL can take 1..N sources as input and can produce 1..N output.
 *
 * @param context runtime configuration
 * @tparam T Type used to capture data changes in the ETL
 * @tparam C Configuration type
 */
abstract class ETL[T, C <: Configuration](context: ETLContext[T, C]) {

  def mainDestination: DatasetConf

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  implicit val conf: Configuration = context.config
  implicit val spark: SparkSession = context.spark

  val minValue: T = context.dataMinValue
  val defaultCurrentValue: T = context.defaultDataCurrentValue

  /**
   * Reads data from a file system and produces a Map[DatasetConf, DataFrame].
   * This method should avoid transformation and joins but can implement filters in order to make the ETL more efficient.
   *
   * @return all the data needed to pass to the transform method and produce the desired output.
   */
  def extract(lastRunValue: T = minValue,
              currentRunValue: T = defaultCurrentValue): Map[String, DataFrame]

  /**
   * Takes a Map[DatasetConf, DataFrame] as input and applies a set of transformations to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data input data
   * @return
   */
  def transform(data: Map[String, DataFrame],
                lastRunValue: T = minValue,
                currentRunValue: T = defaultCurrentValue): Map[String, DataFrame]

  /**
   * Loads the output data into a persistent storage.
   * The output destination can be any of: object store, database or flat files...
   *
   * @param data output data produced by the transform method.
   */
  def load(data: Map[String, DataFrame],
           lastRunValue: T = minValue,
           currentRunValue: T = defaultCurrentValue): Map[String, DataFrame] = {
    data.map { case (dsid, df) =>
      val datasetConf = conf.getDataset(dsid)
      dsid -> loadDataset(df, datasetConf)
    }
  }

  def loadDataset(df: DataFrame, ds: DatasetConf): DataFrame = {
    ds.table.foreach(table => spark.sql(s"CREATE DATABASE IF NOT EXISTS ${table.database}"))
    val repartitionFunc = ds.repartition.getOrElse(this.defaultRepartition)
    val dsWithReplaceWhere = replaceWhere.map(r => ds.copy(writeoptions = ds.writeoptions + ("replaceWhere" -> r))).getOrElse(ds)
    LoadResolver
      .write(spark, conf)(dsWithReplaceWhere.format -> dsWithReplaceWhere.loadtype)
      .apply(dsWithReplaceWhere, repartitionFunc(df))

    log.info(s"Succeeded to load ${ds.id}")
    dsWithReplaceWhere.read
  }

  /**
   * OPTIONAL - Contains all actions needed to be done in order to make the data available to users
   * like creating a view with the data.
   *
   */
  def publish(): Unit = {

  }

  /**
   * Entry point of the etl - execute this method in order to run the whole ETL
   *
   */
  def run(lastRunValue: Option[T] = None,
          currentRunValue: Option[T] = None): Map[String, DataFrame] = {
    val runSteps = context.runSteps
    if (runSteps.isEmpty)
      log.info(s"WARNING ETL started with no runSteps. Nothing will be executed.")
    else
      log.info(s"RUN steps: \t\t ${runSteps.mkString(" -> ")}")

    val runtimeLastRunValue: T = lastRunValue.getOrElse(if (runSteps.contains(RunStep.reset)) minValue else getLastRunValue(mainDestination))
    val runtimeCurrentRunValue: T = currentRunValue.getOrElse(defaultCurrentValue)

    log.info(s"RUN lastRunValue: \t $lastRunValue")
    log.info(s"RUN currentRunValue: \t $currentRunValue")

    if (runSteps.contains(RunStep.reset)) this.reset()

    val data: Map[String, DataFrame] =
      if (runSteps.contains(RunStep.extract) && runSteps.contains(RunStep.sample)) {
        extract(runtimeLastRunValue, runtimeCurrentRunValue).map { case (ds, df) => ds -> sampling(ds).apply(df) }
      } else if (runSteps.contains(RunStep.extract)) {
        extract(runtimeLastRunValue, runtimeCurrentRunValue)
      } else {
        Map()
      }

    val output: Map[String, DataFrame] =
      if (runSteps.contains(RunStep.transform)) {
        transform(data, runtimeLastRunValue, runtimeCurrentRunValue)
      } else {
        Map()
      }

    if (runSteps.contains(RunStep.load)) {
      load(output, runtimeLastRunValue, runtimeCurrentRunValue)
    } else {
      output.foreach { case (dsid, df) =>
        log.info(s"$dsid:")
        df.show(false) //triggers extract and transform method in case load method is not called
      }
    }

    if (runSteps.contains(RunStep.publish)) {
      publish()
    }
    //read all outputDf
    output.keys.toList
      .map(dsid => dsid -> Try(conf.getDataset(dsid).read).getOrElse(spark.emptyDataFrame))
      .toMap
  }

  /**
   * If possible, fetch the last run value from the dataset passed in argument. Usually a date or an id.
   *
   * @param ds dataset
   * @return the last run value or the [[minValue]]
   */
  def getLastRunValue(ds: DatasetConf): T = {
    import spark.implicits._
    val StringType = classTag[String]
    val LocalDateTimeType = classTag[LocalDateTime]

    context.ETLType match {
      case StringType =>
        Try(
          ds.read.select(max(col(ds.writeoptions(UPDATED_ON_COLUMN_NAME)))).limit(1).as[String].head().asInstanceOf[T]
        ).getOrElse(minValue)

      case LocalDateTimeType =>
        ds.loadtype match {
          case Scd1 =>
            Try(
              ds.read.select(max(col(ds.writeoptions(UPDATED_ON_COLUMN_NAME)))).limit(1).as[Timestamp].head().toLocalDateTime.asInstanceOf[T]
            ).getOrElse(minValue)

          case Scd2 =>
            Try(
              ds.read.select(max(col(ds.writeoptions(VALID_FROM_COLUMN_NAME)))).limit(1).as[Date].head().toLocalDate.atStartOfDay().asInstanceOf[T]
            ).getOrElse(minValue)

          case Insert =>
            Try(
              ds.read.select(max(col(ds.writeoptions(UPDATED_ON_COLUMN_NAME)))).limit(1).as[Date].head().toLocalDate.atStartOfDay().asInstanceOf[T]
            ).getOrElse(minValue)

          case OverWritePartition =>
            Try(
              ds.read.select(max(col(ds.partitionby.head))).limit(1).as[Date].head().toLocalDate.atStartOfDay().asInstanceOf[T]
            ).getOrElse(minValue)

          case _ => minValue
        }

      case _ => minValue
    }
  }

  /**
   * Reset the ETL by removing the destination dataset.
   */
  def reset(): Unit = {
    FileSystemResolver
      .resolve(conf.getStorage(mainDestination.storageid).filesystem)
      .remove(mainDestination.path)

    SqlBinderResolver.drop(spark, conf)(mainDestination.format).apply(mainDestination)

  }

  /**
   * Logic used when the ETL is run as a [[RunStep.sample]] step.
   *
   * @return
   */
  def sampling: PartialFunction[String, DataFrame => DataFrame] = defaultSampling

  def defaultSampling: PartialFunction[String, DataFrame => DataFrame] = {
    case _ => df => df.sample(0.05)
  }

  def defaultRepartition: DataFrame => DataFrame = IdentityRepartition

  /**
   * replaceWhere is used in for OverWriteStaticPartition load. It avoids to compute dataframe to infer which partitions to replace.
   * Most of the time, these partitions can be inferred statically. Always prefer that to dynamically overwrite partitions.
   *
   * @return
   */
  def replaceWhere: Option[String] = None

  def toMain(df: => DataFrame): Map[String, DataFrame] = Map(mainDestination.id -> df)
}

