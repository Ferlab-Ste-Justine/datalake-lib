package bio.ferlab.datalake.core.transformation

import org.apache.spark.sql.DataFrame

case class Custom(customTransformation: DataFrame => DataFrame) extends Transformation {
  override def transform: DataFrame => DataFrame = customTransformation
}
