package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

case class SHA1(override val columns: String*) extends HashTransformation {
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df){ case (d, column) =>
      d.withColumn(column, sha1(col(column).cast(StringType)))
    }
  }
}

