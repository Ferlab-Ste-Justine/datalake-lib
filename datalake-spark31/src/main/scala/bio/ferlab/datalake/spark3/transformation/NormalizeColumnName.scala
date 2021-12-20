package bio.ferlab.datalake.spark3.transformation

import bio.ferlab.datalake.spark3.transformation.NormalizeColumnName.normalize
import org.apache.spark.sql.DataFrame

class NormalizeColumns(columns: String*) extends Transformation {
  /**
   * Main method of the trait.
   * It defines the logic to transform the input dataframe.
   *
   * @return a transformed dataframe
   */
  override def transform: DataFrame => DataFrame = {df =>
    columns.foldLeft(df)((d, c) => d.withColumnRenamed(c, normalize(c)))
  }
}

object NormalizeColumnName {
  val normalize: String => String = _.replaceAll("[^a-zA-Z0-9_]", "_").replaceAll("_{2,}", "_")
}
