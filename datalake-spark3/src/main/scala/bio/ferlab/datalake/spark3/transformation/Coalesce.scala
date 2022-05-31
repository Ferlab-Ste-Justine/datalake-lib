package bio.ferlab.datalake.spark3.transformation
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.{Column, DataFrame}

case class Coalesce(values: Seq[Column], columns: String*) extends Transformation {
  /**
   * Main method of the trait.
   * It defines the logic to transform the input dataframe.
   *
   * @return a transformed dataframe
   */
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df){ case (d, c) => d.withColumn(c, coalesce(values:_*))}
  }
}
