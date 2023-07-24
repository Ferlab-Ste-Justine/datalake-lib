package bio.ferlab.datalake.spark3.etl.v3

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, ETLContext}
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

class TransformationsETL[T <: Configuration](context: ETLContext[T],
                                             val source: DatasetConf,
                                             override val mainDestination: DatasetConf,
                                             val transformations: List[Transformation])
  extends SingleETL(context) {

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    log.info(s"extracting: ${source.location}")
    Map(source.id -> spark.read.format(source.format.sparkFormat).options(source.readoptions).load(source.location))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime): DataFrame = {
    log.info(s"transforming: ${source.id} to ${mainDestination.id}")
    //apply list of transformations to the input data
    val finalDf = Transformation.applyTransformations(data(source.id), transformations).persist()

    log.info(s"unique ids: ${finalDf.dropDuplicates(mainDestination.keys).count()}")
    log.info(s"rows: ${finalDf.count()}")
    finalDf
  }
}


