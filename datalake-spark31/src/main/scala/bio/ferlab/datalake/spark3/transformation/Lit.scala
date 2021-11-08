package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

case class Lit(value: Any, columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df) {
      case (d, column) => d.withColumn(column, lit(value))
    }
  }
}

