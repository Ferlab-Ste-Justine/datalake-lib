package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class InputFileName(columnName: String, regex: Option[String] = None) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>
    regex.fold(
      df.withColumn(columnName, input_file_name())
    )(rg =>
      df.withColumn(columnName, regexp_extract(input_file_name(), rg, 1))
    )
  }
}

