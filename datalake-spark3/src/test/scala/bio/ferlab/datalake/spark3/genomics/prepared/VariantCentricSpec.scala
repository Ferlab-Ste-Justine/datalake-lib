package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.enriched.{EnrichedConsequences, EnrichedVariant}
import bio.ferlab.datalake.testutils.models.prepared.PreparedVariantCentric
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame


class VariantCentricSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")

  val data: Map[String, DataFrame] = Map(
    enriched_variants.id -> Seq(
      EnrichedVariant(genes = List(EnrichedVariant.GENES(), EnrichedVariant.GENES(symbol = Some("gene2")))),
      EnrichedVariant(chromosome = "2")
    ).toDF,
    enriched_consequences.id -> Seq(
      EnrichedConsequences(),
      EnrichedConsequences(`symbol` = null, `ensembl_transcript_id` = "transcript2"),
      EnrichedConsequences(`symbol` = "gene2", `ensembl_transcript_id` = "transcript3"),
      EnrichedConsequences(`symbol` = "gene2", `ensembl_transcript_id` = "transcript4", `impact_score` = 10),
      EnrichedConsequences(chromosome = "2")
    ).toDF,

  )

  "transformSingle" should "return data in expected format" in {

    val df = new VariantCentric(TestETLContext()).transformSingle(data)

    val result = df.as[PreparedVariantCentric].collect()

    result.length shouldBe 2
    result.find(_.`chromosome` == "1") shouldBe Some(PreparedVariantCentric())
    result.find(_.`chromosome` == "2") shouldBe Some(PreparedVariantCentric(`chromosome` = "2", `genes` = Set(PreparedVariantCentric.GENES()), max_impact_score = 2))

  }

}
