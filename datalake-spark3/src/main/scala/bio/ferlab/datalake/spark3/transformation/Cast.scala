package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, IntegerType, LongType}

case class Cast(dataType: DataType, columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>
    columns.foldLeft(df){ case (d, column) =>
      d.withColumn(column, col(column).cast(dataType))
    }
  }
}

object Cast {

  /**
   * Cast a column while preserving its original name
   */
  val cast: String => DataType => Column = c => t => col(c).cast(t).as(c)

  val castLong: String => Column = cast(_)(LongType)
  val castInt: String => Column = cast(_)(IntegerType)
  val castFloat: String => Column = cast(_)(FloatType)
  val castDouble: String => Column = cast(_)(DoubleType)
}

