package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

class RawFileToNormalizedETL(override val source: DatasetConf,
                             override val destination: DatasetConf,
                             override val transformations: List[Transformation])
                            (override implicit val conf: Configuration) extends RawToNormalizedETL(source, destination, transformations) {

  private var processedFiles: List[String] = List()

  /**
   * Takes a Map[DataSource, DataFrame] as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data input data
   * @param spark an instance of SparkSession
   * @return
   */
  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime,
                         currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
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
   * OPTIONAL - Contains all actions needed to be done in order to make the data available to users
   * like creating a view with the data.
   * @param spark an instance of SparkSession
   */
  override def publish()(implicit spark: SparkSession): Unit = {
    log.info(s"moving files: \n${processedFiles.mkString("\n")}")
    val files = processedFiles
    Try {
      files.foreach(file =>
        FileSystemResolver
          .resolve(conf.getStorage(source.storageid).filesystem)
          .move(file, file.replace("landing", "archive"), overwrite = true)
      )
      processedFiles = List.empty[String]
    } match {
      case Success(_) => log.info("SUCCESS")
      case Failure(exception) => log.error(s"FAILURE: ${exception.getLocalizedMessage}")
    }

  }
}

