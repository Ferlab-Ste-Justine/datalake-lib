package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class UpperCase(columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df){ case (d, column) =>
      d.withColumn(column, upper(col(column)))
    }
  }
}

