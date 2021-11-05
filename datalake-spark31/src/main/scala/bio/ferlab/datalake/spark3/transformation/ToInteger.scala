package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

case class ToInteger(columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame =  Cast(IntegerType, columns:_*).transform
}

