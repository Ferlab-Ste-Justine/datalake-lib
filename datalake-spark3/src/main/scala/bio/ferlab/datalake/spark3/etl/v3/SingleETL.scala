package bio.ferlab.datalake.spark3.etl.v3

import bio.ferlab.datalake.commons.config.Configuration
import bio.ferlab.datalake.spark3.etl.ETLContext
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

abstract class SingleETL[T <: Configuration](context: ETLContext[T]) extends ETL(context) {

  /**
   * Takes aDataFrame as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data input data
   * @return
   */
  def transformSingle(data: Map[String, DataFrame],
                      lastRunDateTime: LocalDateTime = minDateTime,
                      currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame

  override final def transform(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    toMain(transformSingle(data, lastRunDateTime, currentRunDateTime))

  def loadSingle(data: DataFrame,
                 lastRunDateTime: LocalDateTime = minDateTime,
                 currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    super.loadDataset(data, mainDestination)
  }

  override final def load(data: Map[String, DataFrame],
                          lastRunDateTime: LocalDateTime,
                          currentRunDateTime: LocalDateTime): Map[String, DataFrame] = toMain {
    loadSingle(data(mainDestination.id), lastRunDateTime, currentRunDateTime)
  }
}


