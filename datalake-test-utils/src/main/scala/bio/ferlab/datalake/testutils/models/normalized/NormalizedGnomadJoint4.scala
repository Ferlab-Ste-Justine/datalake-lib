package bio.ferlab.datalake.testutils.models.normalized

case class NormalizedGnomadJoint4 (
  chromosome: String = "1",
  start: Long = 69897,
  end: Long = 69899,
  reference: String = "T",
  alternate: String = "C",
  qual: Double = 0.0,
  name: String = "BRAF",
  ac_genomes: Long = 1,
  af_genomes: Double = 1.0,
  an_genomes: Long = 10,
  hom_genomes: Long = 20,
  ac_exomes: Long = 2,
  af_exomes: Double = 2.0,
  an_exomes: Long = 20,
  hom_exomes: Long = 10,
  ac_joint: Long = 3,
  af_joint: Double = 4.0,
  an_joint: Long = 40,
  hom_joint: Long = 15,
)
