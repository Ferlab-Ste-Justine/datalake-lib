package bio.ferlab.datalake.testutils.models.enriched

case class EnrichedRareVariantInput(chromosome: String = "1",
                                    start: Long = 210862942,
                                    reference: String = "GGCA",
                                    alternate: String = "G",
                                    af: Double = 1.0
                                   ) {

}

case class EnrichedRareVariantOutput(chromosome: String = "1",
                                     start: Long = 210862942,
                                     reference: String = "GGCA",
                                     alternate: String = "G",
                                     af: Double = 1.0,
                                     is_rare: Boolean = false
                                    ) {

}
