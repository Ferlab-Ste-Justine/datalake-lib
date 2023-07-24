package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.enriched.{EnrichedRareVariantInput, EnrichedRareVariantOutput, EnrichedSpliceAi, MAX_SCORE}
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedSpliceAi
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{TestETLContext, WithSparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RareVariantSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers {

  import spark.implicits._

  val job = new RareVariant(TestETLContext())

  val gnomad_df: DatasetConf = job.gnomad

  "transformSingle" should "transform Gnomad v2 to rare variant" in {
    val inputData = Map(
      gnomad_df.id -> Seq(
        EnrichedRareVariantInput(chromosome = "1", start = 1000, reference = "A", alternate = "T", af = 0.005),
        EnrichedRareVariantInput(chromosome = "1", start = 1000, reference = "A", alternate = "T", af = 0.03),
        EnrichedRareVariantInput(chromosome = "1", start = 2000, reference = "A", alternate = "T", af = 0.011),
        EnrichedRareVariantInput(chromosome = "2", start = 1000, reference = "A", alternate = "T", af = 0.005),
        EnrichedRareVariantInput(chromosome = "2", start = 1000, reference = "A", alternate = "T", af = 0.01)

      )
        .toDF()

    )

    val resultDF = job.transformSingle(inputData)

    val expected = Seq(
      EnrichedRareVariantOutput(chromosome = "1", start = 1000, reference = "A", alternate = "T", af = 0.03, is_rare = false),
      EnrichedRareVariantOutput(chromosome = "1", start = 2000, reference = "A", alternate = "T", af = 0.011, is_rare = false),
      EnrichedRareVariantOutput(chromosome = "2", start = 1000, reference = "A", alternate = "T", af = 0.01, is_rare = true)
    )
    resultDF.as[EnrichedRareVariantOutput].collect() should contain theSameElementsAs expected
  }


}
