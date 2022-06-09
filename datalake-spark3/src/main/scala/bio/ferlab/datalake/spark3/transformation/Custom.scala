package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame

case class Custom(customTransformation: DataFrame => DataFrame) extends Transformation {
  override def transform: DataFrame => DataFrame = customTransformation
}
