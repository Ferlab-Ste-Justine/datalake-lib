package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_utc_timestamp}
import org.apache.spark.sql.types.TimestampType

case class ToUtcTimestamps(format: String, columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    val cols = if (columns.isEmpty) {
      df.schema.fields.filter(_.dataType == TimestampType).map(_.name).toSeq
    } else {
      columns.toSeq
    }
    cols.foldLeft(df)((d, c) =>  d.withColumn(c, to_utc_timestamp(col(c), format)))
  }
}
