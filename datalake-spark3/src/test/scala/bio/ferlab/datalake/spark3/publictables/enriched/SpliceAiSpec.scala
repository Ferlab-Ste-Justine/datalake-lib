package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.enriched.{EnrichedSpliceAi, MAX_SCORE}
import bio.ferlab.datalake.testutils.models.normalized.NormalizedSpliceAi
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class SpliceAiSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("normalized_spliceai_snv")
  val destination: DatasetConf = conf.getDataset("enriched_spliceai_snv")

  val job = SpliceAi(TestETLContext(), variantType = "snv")

  "transformSingle" should "transform NormalizedSpliceAi to EnrichedSpliceAi" in {
    val inputData = Map(source.id -> Seq(NormalizedSpliceAi("1"), NormalizedSpliceAi("2")).toDF())

    val resultDF = job.transformSingle(inputData)

    //    ClassGenerator
    //      .writeCLassFile(
    //        "bio.ferlab.datalake.testutils.models.enriched",
    //        "EnrichedSpliceAi",
    //        resultDF,
    //        "datalake-spark3/src/test/scala/")

    val expected = Seq(EnrichedSpliceAi("1"), EnrichedSpliceAi("2"))
    resultDF.as[EnrichedSpliceAi].collect() shouldBe expected
  }

  "transformSingle" should "compute max score for each variant-gene" in {
    val inputData = Map(
      source.id -> Seq(
        NormalizedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 2.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
        NormalizedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene2", `ds_ag` = 0.0, `ds_al` = 0.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
        NormalizedSpliceAi(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 1.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
        NormalizedSpliceAi(`chromosome` = "3", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 1.00, `ds_dg` = 1.0, `ds_dl` = 1.0),
      ).toDF()
    )

    val resultDF = job.transformSingle(inputData)
    resultDF.show(false)

    val expected = Seq(
      MAX_SCORE(`ds` = 2.00, `type` = Some(Seq("AL"))),
      MAX_SCORE(`ds` = 0.00, `type` = None),
      MAX_SCORE(`ds` = 1.00, `type` = Some(Seq("AG", "AL"))),
      MAX_SCORE(`ds` = 1.00, `type` = Some(Seq("AG", "AL", "DG", "DL"))),
    )

    resultDF
      .select("max_score.*")
      .as[MAX_SCORE].collect() shouldBe expected
  }
}
