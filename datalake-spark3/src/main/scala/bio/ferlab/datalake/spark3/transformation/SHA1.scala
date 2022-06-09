package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

case class SHA1(salt: String, override val columns: String*) extends HashTransformation {
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df){ case (d, column) =>
      d.withColumn(column,
        when(col(column).isNull, nullValues)
          .otherwise(
            if(salt.nonEmpty) sha1(concat_ws("_", col(column).cast(StringType), lit(salt)))
            else sha1(col(column).cast(StringType))
          ))
    }
  }
}

