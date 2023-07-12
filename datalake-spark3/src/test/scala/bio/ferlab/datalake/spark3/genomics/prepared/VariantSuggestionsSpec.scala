package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.enriched.{EnrichedConsequences, EnrichedVariant}
import bio.ferlab.datalake.spark3.testmodels.prepared.{PreparedVariantSugggestions, SUGGEST}
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.WithSparkSession
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class VariantSuggestionsSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")

  val data: Map[String, DataFrame] = Map(
    enriched_variants.id -> Seq(
      EnrichedVariant(genes = List(EnrichedVariant.GENES(), EnrichedVariant.GENES(symbol = Some("gene2")))),
      EnrichedVariant(chromosome = "2", hgvsg = "chr2:g.69897T>C")
    ).toDF,
    enriched_consequences.id -> Seq(
      EnrichedConsequences(),
      EnrichedConsequences(`symbol` = null, `ensembl_transcript_id` = "transcript2"),
      EnrichedConsequences(`symbol` = "gene2", `ensembl_transcript_id` = "transcript3"),
      EnrichedConsequences(`symbol` = "gene2", `ensembl_transcript_id` = "transcript4", `impact_score` = 10, refseq_mrna_id = Seq("refseq_mrna_id1", "refseq_mrna_id2")),
      EnrichedConsequences(chromosome = "2")
    ).toDF,

  )

  "transformSingle" should "return data in expected format" in {

    val df = new VariantsSuggestions().transformSingle(data)
    val result = df.as[PreparedVariantSugggestions].collect()
    result.length shouldBe 2
    result.find(_.`chromosome` == "1") shouldBe Some(PreparedVariantSugggestions())
    result.find(_.`chromosome` == "2") shouldBe Some(PreparedVariantSugggestions(
      `chromosome` = "2", `suggestion_id` = "31ecc1636a9f6af0ef2561dc4d1f559e77fb4fa2",
      `locus` = "2-69897-T-C", `rsnumber` = "rs200676709",
      `symbol_aa_change` = Seq("OR4F5 p.Ser269="),
      `hgvsg` = "chr2:g.69897T>C",
      `suggest` = Seq(
        SUGGEST(Seq("chr2:g.69897T>C", "2-69897-T-C", "rs200676709", "257668"), 4),
        SUGGEST(Seq("p.Ser269=", "OR4F5 p.Ser269=", "ENSG00000186092", "ENST00000335137", "NM_001005484.1", "NM_001005484.2", "NP_001005277"), 2),
      )
    ))

  }

}
