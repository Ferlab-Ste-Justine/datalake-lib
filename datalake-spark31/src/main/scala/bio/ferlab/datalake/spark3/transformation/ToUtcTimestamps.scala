package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_utc_timestamp}

case class ToUtcTimestamps(format: String, columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    columns.foldLeft(df)((d, c) =>  d.withColumn(c, to_utc_timestamp(col(c), format)))
  }
}
