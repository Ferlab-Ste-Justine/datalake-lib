package bio.ferlab.datalake.spark3.transformation
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{Column, DataFrame}

case class ConcatWs(name: String, sep: String, cols: Column*) extends Transformation {
  /**
   * Main method of the trait.
   * It defines the logic to transform the input dataframe.
   *
   * @return a transformed dataframe
   */
  override def transform: DataFrame => DataFrame = { df =>
    df.withColumn(name, concat_ws(sep, cols:_*))
  }
}
