package bio.ferlab.datalake.spark3.etl.v2

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

@deprecated("use [[v4.TransformationsETL]] instead", "11.0.0")
class RawToNormalizedETL(val source: DatasetConf,
                         override val mainDestination: DatasetConf,
                         val transformations: List[Transformation])
                        (override implicit val conf: Configuration) extends ETL {

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
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
  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime,
                         currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    log.info(s"transforming: ${source.id} to ${mainDestination.id}")
    //apply list of transformations to the input data
    val finalDf = Transformation.applyTransformations(data(source.id), transformations).persist()

    log.info(s"unique ids: ${finalDf.dropDuplicates(mainDestination.keys).count()}")
    log.info(s"rows: ${finalDf.count()}")
    Map(mainDestination.id -> finalDf)
  }
}


