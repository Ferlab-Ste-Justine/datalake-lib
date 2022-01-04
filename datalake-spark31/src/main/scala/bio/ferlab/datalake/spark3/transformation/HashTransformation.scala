package bio.ferlab.datalake.spark3.transformation

trait HashTransformation extends Transformation {self =>
  val columns: Seq[String]
}
