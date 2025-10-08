package bio.ferlab.datalake.testutils.models.raw

case class RawTopMedFreeze8(
                              contigName: String = "chr1",
                              start: Long = 69896,
                              end: Long = 69897,
                              names: List[String] = List("TOPMed_freeze_5?chr1:10,051"),
                              referenceAllele: String = "T",
                              alternateAlleles: List[String] = List("C"),
                              qual: Double = 255.0,
                              filters: List[String] = List("SVM", "MIS2"),
                              splitFromMultiAllelic: Boolean = false,
                              INFO_AF: List[Double] = List(1.59276E-5),
                              INFO_AC: List[Int] = List(2),
                              INFO_Het: List[Int] = List(2),
                              INFO_Hom: List[Int] = List(0),
                              INFO_NS: Int = 132345,
                              INFO_VRT: Int = 1 ,
                              INFO_AN: Int = 125568,
                              genotypes: List[GENOTYPES] = null
                            )



