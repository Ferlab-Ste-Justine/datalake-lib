package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.enriched.{EnrichedRareVariant, EnrichedSpliceAi, MAX_SCORE}
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedSpliceAi
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RareVariantSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  val job = new RareVariant()

  val gnomad_genomes_v2_1: DatasetConf = job.gnomad_genomes_v2_1

  "transformSingle" should "transform Gnomad v2 to rare variant" in {
    val inputData = Map(
      gnomad_genomes_v2_1.id -> Seq(
        EnrichedRareVariant(chromosome = "1", start = 1000, reference = "A", alternate = "T", af = 0.01),
        EnrichedRareVariant(chromosome = "1", start = 1000, reference = "A", alternate = "T", af = 0.005),
        EnrichedRareVariant(chromosome = "1", start = 1000, reference = "A", alternate = "T", af = 0.03),
        EnrichedRareVariant(chromosome = "1", start = 2000, reference = "A", alternate = "T", af = 0.011),
        EnrichedRareVariant(chromosome = "2", start = 1000, reference = "A", alternate = "T", af = 0.005)

      )
        .toDF()

    )

    val resultDF = job.transformSingle(inputData)

    val expected = Seq(
      EnrichedRareVariant(chromosome = "1", start = 1000, reference = "A", alternate = "T", af = 0.01),
      EnrichedRareVariant(chromosome = "2", start = 1000, reference = "A", alternate = "T", af = 0.005)
    )
    resultDF.as[EnrichedRareVariant].collect() shouldBe expected
  }


}
