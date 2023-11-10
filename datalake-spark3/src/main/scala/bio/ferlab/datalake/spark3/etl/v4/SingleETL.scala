package bio.ferlab.datalake.spark3.etl.v4

import bio.ferlab.datalake.commons.config.{Configuration, ETLContext}
import org.apache.spark.sql.DataFrame

abstract class SingleETL[T, C <: Configuration](context: ETLContext[T, C]) extends ETL(context) {

  /**
   * Takes a DataFrame as input and applies a set of transformations to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data input data
   * @return
   */
  def transformSingle(data: Map[String, DataFrame],
                      lastRunValue: T = minValue,
                      currentRunValue: T = defaultCurrentValue): DataFrame

  override final def transform(data: Map[String, DataFrame],
                               lastRunValue: T = minValue,
                               currentRunValue: T = defaultCurrentValue): Map[String, DataFrame] =
    toMain(transformSingle(data, lastRunValue, currentRunValue))

  def loadSingle(data: DataFrame,
                 lastRunValue: T = minValue,
                 currentRunValue: T = defaultCurrentValue): DataFrame = {
    super.loadDataset(data, mainDestination)
  }

  override final def load(data: Map[String, DataFrame],
                          lastRunValue: T,
                          currentRunValue: T): Map[String, DataFrame] = toMain {
    loadSingle(data(mainDestination.id), lastRunValue, currentRunValue)
  }
}


