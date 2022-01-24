package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_utc_timestamp}

case class FromUtcTimestamps(format: String, columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    columns.foldLeft(df)((d, c) =>  d.withColumn(c, from_utc_timestamp(col(c), format)))
  }
}