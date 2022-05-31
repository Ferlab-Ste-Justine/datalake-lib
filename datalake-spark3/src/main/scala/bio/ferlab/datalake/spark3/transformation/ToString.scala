package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

case class ToString(columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = Cast(StringType, columns:_*).transform
}

