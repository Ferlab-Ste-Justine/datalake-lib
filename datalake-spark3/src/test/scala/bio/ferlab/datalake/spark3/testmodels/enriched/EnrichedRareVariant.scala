package bio.ferlab.datalake.spark3.testmodels.enriched

case class EnrichedRareVariant(chromosome: String = "1",
                               start: Long = 210862942,
                               reference: String = "GGCA",
                               alternate: String = "G",
                               af: Double = 1.0
                      ) {

}
