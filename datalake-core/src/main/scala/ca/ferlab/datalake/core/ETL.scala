package ca.ferlab.datalake.core

import ca.ferlab.datalake.core.config.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}


abstract class ETL(val destination: DataSource)(implicit val conf: Configuration) {

  def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame]

  def transform(inputData: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame

  def load(data: DataFrame)(implicit spark: SparkSession): Unit

  def publish()(implicit spark: SparkSession): Unit

  def run()(implicit spark: SparkSession): Unit = {
    val inputs = extract()
    val output = transform(inputs)
    load(output)
    publish()
  }

}
