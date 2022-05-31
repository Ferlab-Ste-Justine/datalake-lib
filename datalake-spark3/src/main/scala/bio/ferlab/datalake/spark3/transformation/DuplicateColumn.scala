package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class DuplicateColumn(source: String, destination: String) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    df.withColumn(destination, col(source))
  }
}

