package bio.ferlab.datalake.spark3.genomics.normalized

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{annotations, csq}
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedConsequences
import bio.ferlab.datalake.spark3.testmodels.raw.{InfoCSQ, RawVcf, RawVcfWithInfoAnn}
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime

class BaseConsequencesSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  class ConsequenceEtl(annotationsColumn: Column = csq, groupByLocus: Boolean = true) extends BaseConsequences(TestETLContext(), annotationsColumn, groupByLocus) {
    override def extract(lastRunDateTime: LocalDateTime, currentRunDateTime: LocalDateTime): Map[String, DataFrame] = ???
  }

  "consequences job" should "transform data in expected format" in {
    val jobWithInfoCsq = new ConsequenceEtl(groupByLocus = false)
    val data = Map(
      jobWithInfoCsq.raw_vcf -> Seq(RawVcf()).toDF()
    )
    val results = jobWithInfoCsq.transform(data)
    val resultDf = results(jobWithInfoCsq.mainDestination.id)
    val result = resultDf.as[NormalizedConsequences].collect().head

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedConsequences", resultDf, "src/test/scala/")
    result shouldBe
      NormalizedConsequences(
        `created_on` = result.`created_on`,
        `updated_on` = result.`updated_on`,
        `normalized_consequences_oid` = result.`normalized_consequences_oid`)
  }

  it should "transform data with column INFO_ANN in vcf in expected format" in {
    val jobWithInfoAnn = new ConsequenceEtl(groupByLocus = false, annotationsColumn = annotations)
    val data = Map(
      jobWithInfoAnn.raw_vcf -> Seq(RawVcfWithInfoAnn()).toDF()
    )
    val results = jobWithInfoAnn.transform(data)
    val resultDf = results(jobWithInfoAnn.mainDestination.id)
    val result = resultDf.as[NormalizedConsequences].collect().head

    //ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "NormalizedConsequences", resultDf, "src/test/scala/")
    result shouldBe
      NormalizedConsequences(
        `created_on` = result.`created_on`,
        `updated_on` = result.`updated_on`,
        `normalized_consequences_oid` = result.`normalized_consequences_oid`)
  }

  "consequences job" should "remove duplicates" in {
    val jobWithInfoCsq = new ConsequenceEtl(groupByLocus = false)
    val data_with_duplicates = Map(
      jobWithInfoCsq.raw_vcf -> Seq(
        RawVcf(`INFO_CSQ` = List(InfoCSQ(`Feature` = "bar"), InfoCSQ(`Feature` = "bar"))), // duplicate with itself
        RawVcf(`INFO_CSQ` = List(InfoCSQ(`Feature` = "foo"))), // duplicated with below
        RawVcf(`INFO_CSQ` = List(InfoCSQ(`Feature` = "foo"))),
        RawVcf(`INFO_CSQ` = List(InfoCSQ(`Feature` = null))), // duplicated with below
        RawVcf(`INFO_CSQ` = List(InfoCSQ(`Feature` = null))),
      ).toDF()
    )
    val results = jobWithInfoCsq.transform(data_with_duplicates)
    val resultDf = results(jobWithInfoCsq.mainDestination.id)
    val result = resultDf.as[NormalizedConsequences].collect()

    result should contain theSameElementsAs Seq(
      NormalizedConsequences(
        `ensembl_feature_id` = "bar",
        `ensembl_transcript_id` = "bar",
        `created_on` = result.head.`created_on`,
        `updated_on` = result.head.`updated_on`,
        `normalized_consequences_oid` = result.head.`normalized_consequences_oid`),
      NormalizedConsequences(
        `ensembl_feature_id` = "foo",
        `ensembl_transcript_id` = "foo",
        `created_on` = result.head.`created_on`,
        `updated_on` = result.head.`updated_on`,
        `normalized_consequences_oid` = result.head.`normalized_consequences_oid`),
      NormalizedConsequences(
        `ensembl_feature_id` = null,
        `ensembl_transcript_id` = null,
        `created_on` = result.head.`created_on`,
        `updated_on` = result.head.`updated_on`,
        `normalized_consequences_oid` = result.head.`normalized_consequences_oid`)
    )
  }

  it should "remove duplicated locus with groupByColumn" in {
    val jobWithInfoCsq = new ConsequenceEtl(groupByLocus = true)
    val data_with_duplicates = Map(
      jobWithInfoCsq.raw_vcf -> Seq(
        RawVcf(`INFO_CSQ` = List(InfoCSQ(`Feature` = "bar"), InfoCSQ(`Feature` = "bar"))), // duplicate with itself
        RawVcf(`INFO_CSQ` = List(InfoCSQ(`Feature` = "foo"))) // duplicate locus
      ).toDF()
    )
    val results = jobWithInfoCsq.transform(data_with_duplicates)
    val resultDf = results(jobWithInfoCsq.mainDestination.id)
    val result = resultDf.as[NormalizedConsequences].collect()

    result should contain theSameElementsAs Seq(
      NormalizedConsequences(
        `ensembl_feature_id` = "bar",
        `ensembl_transcript_id` = "bar",
        `created_on` = result.head.`created_on`,
        `updated_on` = result.head.`updated_on`,
        `normalized_consequences_oid` = result.head.`normalized_consequences_oid`)
    )
  }

}
