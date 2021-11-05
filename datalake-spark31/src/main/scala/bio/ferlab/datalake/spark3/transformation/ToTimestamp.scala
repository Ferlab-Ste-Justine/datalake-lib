package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_timestamp}

case class ToTimestamp(format: String, columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    columns.foldLeft(df)((d, c) => d.withColumn(c, to_timestamp(col(c), format)))
  }
}
