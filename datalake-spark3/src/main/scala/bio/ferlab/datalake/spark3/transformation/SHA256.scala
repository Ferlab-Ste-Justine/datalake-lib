package bio.ferlab.datalake.spark3.transformation

import bio.ferlab.datalake.spark3.transformation.HashTransformation.SimpleHashTransformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

case class SHA256(salt: String, override val columns: String*) extends SimpleHashTransformation {
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df){ case (d, column) =>
      d.withColumn(column,
        when(col(column).isNull, nullValues)
          .otherwise(sha2(concat_ws("_", col(column).cast(StringType), lit(salt)), 256)))
    }
  }
}

