package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType

case class ToDouble(columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame =  Cast(DoubleType, columns:_*).transform
}

