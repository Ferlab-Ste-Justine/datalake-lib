package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType

case class ToLong(columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = Cast(LongType, columns:_*).transform
}

