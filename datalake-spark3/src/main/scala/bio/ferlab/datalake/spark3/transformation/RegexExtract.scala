package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_extract}

case class RegexExtract(sourcefield: String, destinationfield: String, regex: String, groupIdx: Int) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>
    df.withColumn(destinationfield, regexp_extract(col(sourcefield), regex, groupIdx))
  }
}

