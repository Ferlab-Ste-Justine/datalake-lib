package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.enriched.{EnrichedSpliceAi, MAX_SCORE}
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedSpliceAi
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpliceAiSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  val job = new SpliceAi()

  val spliceai_indel: DatasetConf = job.spliceai_indel
  val spliceai_snv: DatasetConf = job.spliceai_snv
  val destination: DatasetConf = job.mainDestination

  "transformSingle" should "transform NormalizedSpliceAi to EnrichedSpliceAi" in {
    val inputData = Map(
        spliceai_snv.id -> Seq(NormalizedSpliceAi("1")).toDF(),
        spliceai_indel.id -> Seq(NormalizedSpliceAi("2")).toDF(),
      )

    val resultDF = job.transformSingle(inputData)

//    ClassGenerator
//      .writeCLassFile(
//        "bio.ferlab.datalake.spark3.testmodels.enriched",
//        "EnrichedSpliceAi",
//        resultDF,
//        "datalake-spark3/src/test/scala/")

    val expected = Seq(EnrichedSpliceAi("1"), EnrichedSpliceAi("2"))
    resultDF.as[EnrichedSpliceAi].collect() shouldBe expected
  }

  "transformSingle" should "compute max score for each variant-gene"  in {
    val inputData = Map(
      spliceai_snv.id -> Seq(
        NormalizedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 2.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
        NormalizedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene2", `ds_ag` = 0.0, `ds_al` = 0.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
      ).toDF(),
      spliceai_indel.id -> Seq(
        NormalizedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "AT", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 1.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
      ).toDF(),
    )

    val resultDF = job.transformSingle(inputData)

    val expected = Seq(
      MAX_SCORE(`ds` = 2.00, `type` = Seq("AL")),
      MAX_SCORE(`ds` = 0.00, `type` = Seq("AG", "AL", "DG", "DL")),
      MAX_SCORE(`ds` = 1.00, `type` = Seq("AG", "AL")),
    )

    resultDF
      .select("max_score.*")
      .as[MAX_SCORE].collect() shouldBe expected
  }

}
