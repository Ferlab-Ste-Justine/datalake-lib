package bio.ferlab.datalake.spark3.transformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_remove, col, split, substring}

case class Split(pattern: String, columns: String*) extends Transformation {
  /**
   * Main method of the trait.
   * It defines the logic to transform the input dataframe.
   *
   * @return a transformed dataframe
   */
  override def transform: DataFrame => DataFrame = {df =>
    columns
      .foldLeft(df){ case (d, c) => d.withColumn(c, array_remove(split(col(c), pattern), "")) }

  }
}
