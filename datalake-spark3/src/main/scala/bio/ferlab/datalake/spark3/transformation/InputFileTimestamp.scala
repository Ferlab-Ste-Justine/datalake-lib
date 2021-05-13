package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

case class InputFileTimestamp(columnName: String,
                              regex: String = "(\\d{8}_\\d{6})",
                              format: String = "yyyyMMdd_HHmmss") extends Transformation {
  override def transform: DataFrame => DataFrame = {
    _.withColumn(columnName, to_timestamp(regexp_extract(input_file_name(), regex, 0), format))
  }
}

