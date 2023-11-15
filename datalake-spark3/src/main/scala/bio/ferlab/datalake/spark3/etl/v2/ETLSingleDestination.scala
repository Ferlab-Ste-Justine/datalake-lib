package bio.ferlab.datalake.spark3.etl.v2

import bio.ferlab.datalake.commons.config.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

@deprecated("use [[v4.ETL]] instead", "11.0.0")
abstract class ETLSingleDestination()(implicit conf: Configuration) extends ETL {

  /**
   * Takes aDataFrame as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data  input data
   * @param spark an instance of SparkSession
   * @return
   */
  def transformSingle(data: Map[String, DataFrame],
                      lastRunDateTime: LocalDateTime = minDateTime,
                      currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame

  override final def transform(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] =
    toMain(transformSingle(data, lastRunDateTime, currentRunDateTime))

  def loadSingle(data: DataFrame,
                 lastRunDateTime: LocalDateTime = minDateTime,
                 currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    super.loadDataset(data, mainDestination)
  }

  override final def load(data: Map[String, DataFrame],
                          lastRunDateTime: LocalDateTime,
                          currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = toMain {
    loadSingle(data(mainDestination.id), lastRunDateTime, currentRunDateTime)
  }
}
