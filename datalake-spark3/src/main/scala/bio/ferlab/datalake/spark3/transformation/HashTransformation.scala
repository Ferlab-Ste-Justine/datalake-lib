package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType

sealed trait HashTransformation[A] extends Transformation {self =>
  val columns: A
  val nullValues: Column = lit(null).cast(StringType)
}

object HashTransformation {
  trait SimpleHashTransformation extends HashTransformation[Seq[String]]
  trait DynamicHashTransformation extends HashTransformation[DataFrame => Seq[String]]
}
