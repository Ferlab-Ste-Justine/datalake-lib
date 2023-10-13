package bio.ferlab.datalake.testutils.models.normalized

case class NormalizedSNV(chromosome: String = "1",
                         start: Long = 69897,
                         end: Long = 69898,
                         reference: String = "T",
                         alternate: String = "C",
                         calls: Seq[Int] = Seq(1, 1),
                         affected_status: Boolean = false,
                         genes_symbol: List[String] = List("OR4F5"),
                         hgvsg: String = "chr1:g.69897T>C",
                         name: Option[String] = None,
                         variant_class: String = "SNV",
                         variant_type: String = "germline",
                         zygosity: String = "HOM",
                         study_id: String = "S1",
                         ethnicity: Option[String] = None,
                         participant_id: String = "P1",
                         transmission_mode: String = "AR",
                         study_code: String = "STUDY_CODE_1",
                         has_alt:Boolean = true
                        )