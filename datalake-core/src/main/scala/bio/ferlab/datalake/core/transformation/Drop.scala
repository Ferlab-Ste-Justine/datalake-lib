package bio.ferlab.datalake.core.transformation

import org.apache.spark.sql.DataFrame

case class Drop(columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = {
    _.drop(columns:_*)
  }
}

