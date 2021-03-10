package bio.ferlab.datalake.core.transformation

import org.apache.spark.sql.DataFrame

trait Transformation {
  def transform: DataFrame => DataFrame
}

object Transformation {
  def applyTransformations(df: DataFrame, transformations: List[Transformation]): DataFrame = {
    transformations.foldLeft(df)((d, t) => t.transform(d))
  }
}
