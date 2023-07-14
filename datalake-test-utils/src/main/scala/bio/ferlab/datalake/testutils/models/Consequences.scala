package bio.ferlab.datalake.testutils.models

case class ConsequencesInput(chromosome: String = "1",
                             start: Long = 1,
                             reference: String = "T",
                             alternate: String = "C",
                             ensembl_transcript_id: String = "ENST00000000001",
                             impact_score: Int = 1,
                             symbol: String = "OR4F5",
                             biotype: String = "protein_coding",
                             mane_select: Boolean = false,
                             mane_plus: Boolean = false,
                             canonical: Boolean = false)

case class PickedConsequencesOuput(chromosome: String = "1",
                                   start: Long = 1,
                                   reference: String = "T",
                                   alternate: String = "C",
                                   ensembl_transcript_id: String = "ENST00000000001",
                                   impact_score: Int = 1,
                                   symbol: String = "OR4F5",
                                   biotype: String = "protein_coding",
                                   mane_select: Boolean = false,
                                   mane_plus: Boolean = false,
                                   canonical: Boolean = false,
                                   picked: Option[Boolean] = Some(true))
