package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace}

case class RegexReplace(sourcefield: String, destinationfield: String, pattern: String, replacement: String) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>
    df.withColumn(destinationfield, regexp_replace(col(sourcefield), pattern, replacement))
  }
}
