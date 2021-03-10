package bio.ferlab.datalake.core.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}

case class Date(format: String, columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    columns.foldLeft(df)((d, c) => d.withColumn(c, to_date(col(c), format)))
  }
}

