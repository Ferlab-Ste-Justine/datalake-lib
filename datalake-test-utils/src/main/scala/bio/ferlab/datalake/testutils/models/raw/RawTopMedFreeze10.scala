package bio.ferlab.datalake.testutils.models.raw

case class RawTopMedFreeze10(
                              contigName: String = "chr1",
                              start: Long = 69896,
                              end: Long = 69897,
                              names: Option[List[String]] = None,
                              referenceAllele: String = "T",
                              alternateAlleles: List[String] = List("C"),
                              qual: Double = 255.0,
                              filters: List[String] = List("SVM"),
                              splitFromMultiAllelic: Boolean = false,
                              INFO_AF: List[Double] = List(1.59276E-5),
                              INFO_AC: List[Int] = List(2),
                              INFO_Het: List[Int] = List(2),
                              INFO_Hom: List[Int] = List(0),
                              genotypes: Option[List[GENOTYPES]] = None
                            )



