package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType

trait HashTransformation extends Transformation {self =>
  val columns: Seq[String]
  val nullValues: Column = lit(null).cast(StringType)
}
