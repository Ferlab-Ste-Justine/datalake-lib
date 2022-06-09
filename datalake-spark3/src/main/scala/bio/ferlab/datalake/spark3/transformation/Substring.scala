package bio.ferlab.datalake.spark3.transformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, substring}

case class Substring(column: String, pos: Int, len: Int) extends Transformation {
  /**
   * Main method of the trait.
   * It defines the logic to transform the input dataframe.
   *
   * @return a transformed dataframe
   */
  override def transform: DataFrame => DataFrame = {
    _.withColumn(column, substring(col(column), pos, len))
  }
}
