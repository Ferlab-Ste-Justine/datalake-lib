package bio.ferlab.datalake.testutils.models.genomicimplicits

case class RelativesGenotype(chromosome: String = "1",
                             start: Long = 1000,
                             reference: String = "A",
                             alternate: String = "T",
                             participant_id: String = "PT_1",
                             mother_id: Option[String] = None,
                             father_id: Option[String] = None,
                             family_id: Option[String] = None,
                             calls: Seq[Int] = Seq(0, 0),
                             gq: Int = 20,
                             other: Option[String] = None

                            ) {

}

case class RelativesGenotypeOutput(chromosome: String = "1",
                                   start: Long = 1000,
                                   reference: String = "A",
                                   alternate: String = "T",
                                   participant_id: String = "PT_1",
                                   mother_id: Option[String] = None,
                                   father_id: Option[String] = None,
                                   family_id: Option[String] = None,
                                   calls: Seq[Int] = Seq(0, 0),
                                   gq: Int = 20,
                                   other: Option[String] = None,
                                   mother_calls: Option[Seq[Int]] = None,
                                   mother_gq: Option[Int] = None,
                                   mother_other: Option[String] = None,
                                   father_calls: Option[Seq[Int]] = None,
                                   father_gq: Option[Int] = None,
                                   father_other: Option[String] = None,
                                  ) {

}
