package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class SHA256(salt: String, override val columns: String*) extends HashTransformation {
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df){ case (d, column) =>
      d.withColumn(column, sha2(concat_ws("_", col(column), lit(salt)), 256))
    }
  }
}

