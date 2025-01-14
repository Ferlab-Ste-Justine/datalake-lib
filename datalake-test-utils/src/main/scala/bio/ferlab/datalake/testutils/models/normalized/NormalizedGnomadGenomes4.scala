package bio.ferlab.datalake.testutils.models.normalized

case class NormalizedGnomadGenomes4(
  chromosome: String = "chrY",
  start: Long = 69897,
  end: Long = 69899,
  reference: String = "T",
  alternate: String = "C",
  qual: Double = 0.0,
  name: String = "BRAF",
  ac: Long = 2,
  af: Double = 2.0,
  an: Long = 20,
  hom: Long = 10,
)