package bio.ferlab.datalake.testutils.models.genomicimplicits

case class OtherCompoundHetInput(other_patient_id: String,
                                 chromosome: String,
                                 start: Long,
                                 reference: String,
                                 alternate: String,
                                 other_symbols: Seq[String],
                                 parental_origin: Option[String] = None,
                                 zygosity: String = "HET"
                                )

case class HCComplement(symbol: String,
                        locus: Seq[String])

case class CompoundHetOutput(patient_id: String,
                             chromosome: String,
                             start: Long,
                             reference: String,
                             alternate: String,
                             is_hc: Boolean,
                             hc_complement: Seq[HCComplement])


case class PossiblyHCComplement(symbol: String,
                                count: Long)

case class PossiblyCompoundHetOutput(patient_id: String,
                                     chromosome: String,
                                     start: Long,
                                     reference: String,
                                     alternate: String,
                                     is_possibly_hc: Boolean,
                                     possibly_hc_complement: Seq[PossiblyHCComplement])

case class FullCompoundHetOutput(patient_id: String,
                                 chromosome: String,
                                 start: Long,
                                 reference: String,
                                 alternate: String,
                                 is_hc: Boolean,
                                 hc_complement: Seq[HCComplement],
                                 is_possibly_hc: Boolean,
                                 possibly_hc_complement: Seq[PossiblyHCComplement])