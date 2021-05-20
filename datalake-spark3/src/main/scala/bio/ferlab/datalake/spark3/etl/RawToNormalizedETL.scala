package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class RawToNormalizedETL(val source: DatasetConf,
                         override val destination: DatasetConf,
                         val transformations: List[Transformation])
                        (override implicit val conf: Configuration) extends ETL() {

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  private var processedFiles: List[String] = List()

  override def extract()(implicit spark: SparkSession): Map[String, DataFrame] = {
    log.info(s"extracting: ${source.location}")
    Map(source.id -> spark.read.format(source.format.sparkFormat).options(source.readoptions).load(source.location))
  }

  /**
   * Takes a Map[DataSource, DataFrame] as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data input data
   * @param spark an instance of SparkSession
   * @return
   */
  override def transform(data: Map[String, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    log.info(s"transforming: ${source.id} to ${destination.id}")
    //keep in memory which files are being processed
    processedFiles = data(source.id).withColumn("files", input_file_name())
      .select("files").as[String].collect().distinct.toList
    //apply list of transformations to the input data
    val finalDf = Transformation.applyTransformations(data(source.id), transformations).persist()

    log.info(s"unique ids: ${finalDf.dropDuplicates(destination.keys).count()}")
    log.info(s"rows: ${finalDf.count()}")
    finalDf
  }

  /**
   * Loads the output data into a persistent storage.
   * The output destination can be any of: object store, database or flat files...
   *
   * @param data  output data produced by the transform method.
   * @param spark an instance of SparkSession
   */
  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    log.info(s"loading: ${destination.id}")
    Try(
      destination.table.foreach(table => spark.sql(s"CREATE DATABASE IF NOT EXISTS ${table.database}"))
    )

    LoadResolver
      .resolve(spark, conf)(destination.format -> destination.loadtype)
      .apply(destination, data)
  }


  /**
   * OPTIONAL - Contains all actions needed to be done in order to make the data available to users
   * like creating a view with the data.
   * @param spark an instance of SparkSession
   */
  override def publish()(implicit spark: SparkSession): Unit = {
    log.info(s"moving files: \n${processedFiles.mkString("\n")}")
    val files = processedFiles
    Try {
      files.foreach(file =>
        fs.move(file, file.replace("landing", "archive"), overwrite = true)
      )
      processedFiles = List.empty[String]
    } match {
      case Success(_) => log.info("SUCCESS")
      case Failure(exception) => log.error(s"FAILURE: ${exception.getLocalizedMessage}")
    }

  }
}

