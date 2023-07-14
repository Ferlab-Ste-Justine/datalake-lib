package bio.ferlab.datalake.testutils.models.genomicimplicits

case class CompoundHetInput(patient_id: String,
                            chromosome: String,
                            start: Long,
                            reference: String,
                            alternate: String,
                            symbols: Seq[String],
                            parental_origin: Option[String] = None,
                            zygosity: String = "HET"
                           )
