package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.genomics.{FrequencySplit, SimpleAggregation}
import bio.ferlab.datalake.spark3.testmodels.enriched.{EnrichedGenes, EnrichedSpliceAi, EnrichedVariant, MAX_SCORE}
import bio.ferlab.datalake.spark3.testmodels.normalized.{NormalizedClinvar, NormalizedDbsnp, NormalizedGnomadConstraint, NormalizedGnomadExomes211, NormalizedGnomadGenomes211, NormalizedGnomadGenomes3, NormalizedOneKGenomes, NormalizedSNV, NormalizedSpliceAi, NormalizedTopmed}
import bio.ferlab.datalake.spark3.testutils.{WithSparkSession, WithTestConfig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EnrichedVariantsSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val snvKeyId = "snv"
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_constraint: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")
  val gnomad_genomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v2_1_1")
  val gnomad_exomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_v2_1_1")
  val gnomad_genomes_v3: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val spliceai: DatasetConf = conf.getDataset("enriched_spliceai")

  val occurrencesDf: DataFrame = Seq(
    NormalizedSNV(`participant_id` = "PA0001"),
    NormalizedSNV(`participant_id` = "PA0002"),
    NormalizedSNV(`participant_id` = "PA0003", `zygosity` = "WT", `calls` = List(0, 0))
  ).toDF
  val genomesDf: DataFrame = Seq(NormalizedOneKGenomes()).toDF
  val topmed_bravoDf: DataFrame = Seq(NormalizedTopmed()).toDF
  val gnomad_genomes_2_1_1Df: DataFrame = Seq(NormalizedGnomadGenomes211()).toDF
  val gnomad_exomes_2_1_1Df: DataFrame = Seq(NormalizedGnomadExomes211()).toDF
  val gnomad_genomes_3Df: DataFrame = Seq(NormalizedGnomadGenomes3()).toDF
  val dbsnpDf: DataFrame = Seq(NormalizedDbsnp()).toDF
  val clinvarDf: DataFrame = Seq(NormalizedClinvar(chromosome = "1", start = 69897, reference = "T", alternate = "C")).toDF
  val genesDf: DataFrame = Seq(EnrichedGenes()).toDF()
  val spliceaiDf: DataFrame = Seq(EnrichedSpliceAi(chromosome = "1", start = 69897, reference = "T", alternate = "C", symbol = "OR4F5", ds_ag = 0.01, `max_score` = MAX_SCORE(ds = 0.01, `type` = Seq("AG")))).toDF()

  private val data = Map(
    snvKeyId -> occurrencesDf,
    thousand_genomes.id -> genomesDf,
    topmed_bravo.id -> topmed_bravoDf,
    gnomad_genomes_v2_1_1.id -> gnomad_genomes_2_1_1Df,
    gnomad_exomes_v2_1_1.id -> gnomad_exomes_2_1_1Df,
    gnomad_genomes_v3.id -> gnomad_genomes_3Df,
    dbsnp.id -> dbsnpDf,
    clinvar.id -> clinvarDf,
    genes.id -> genesDf,
    spliceai.id -> spliceaiDf,
  )

  "transformSingle" should "return expected result" in {
    val df = new Variants(snvDatasetId = snvKeyId, frequencies = Seq(FrequencySplit("frequency", extraAggregations = Seq(SimpleAggregation(name = "zygosities", c = col("zygosity"))))))
      .transformSingle(data)
    val result = df.as[EnrichedVariant].collect()
    result.length shouldBe 1
    result.head shouldBe EnrichedVariant()
  }
}
