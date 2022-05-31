package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame

trait Transformation {self =>
  /**
   * Main method of the trait.
   * It defines the logic to transform the input dataframe.
   * @return a transformed dataframe
   */
  def transform: DataFrame => DataFrame

  /**
   * Combines two transformations into one and execute the transformation on the left side of the arrow first.
   * @param t transformation to combine with
   * @return a new instance of [[Transformation]]
   */
  def ->(t: Transformation): Transformation = {
    new Transformation {
      override def transform: DataFrame => DataFrame = df => t.transform.apply(self.transform.apply(df))
    }
  }
}

object Transformation {
  /**
   * Defines a way to execute a list of transformations to a DataFrame
   * @param df DataFrame to transform
   * @param transformations list of transformation in order of execution.
   * @return a transformed DataFrame.
   *         Note that the DataFrame execution plan stay unchanged and you still need to call an action
   *         like a write, collect or count to execute the transformations.
   */
  def applyTransformations(df: DataFrame, transformations: List[Transformation]): DataFrame = {
    transformations.foldLeft(df)((d, t) => t.transform(d))
  }
}
