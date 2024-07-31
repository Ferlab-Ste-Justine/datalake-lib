package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.genomics.enriched.Variants.DataFrameOps
import bio.ferlab.datalake.spark3.genomics.{FrequencySplit, SimpleAggregation}
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.enriched.EnrichedVariant.CMC
import bio.ferlab.datalake.testutils.models.enriched.{EnrichedGenes, EnrichedVariant}
import bio.ferlab.datalake.testutils.models.normalized._
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_set, max}

class EnrichedVariantsSpec extends SparkSpec with WithTestConfig {

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
  val cosmic: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")

  val occurrencesDf: DataFrame = Seq(
    NormalizedSNV(`participant_id` = "PA0001", study_id = "S1"),
    NormalizedSNV(`participant_id` = "PA0002", study_id = "S2"),
    NormalizedSNV(`participant_id` = "PA0003", study_id = "S3", `zygosity` = "WT", `calls` = List(0, 0), has_alt = false) // Will be filtered out
  ).toDF
  val genomesDf: DataFrame = Seq(NormalizedOneKGenomes()).toDF
  val topmed_bravoDf: DataFrame = Seq(NormalizedTopmed()).toDF
  val gnomad_genomes_2_1_1Df: DataFrame = Seq(NormalizedGnomadGenomes211()).toDF
  val gnomad_exomes_2_1_1Df: DataFrame = Seq(NormalizedGnomadExomes211()).toDF
  val gnomad_genomes_3Df: DataFrame = Seq(NormalizedGnomadGenomes3()).toDF
  val dbsnpDf: DataFrame = Seq(NormalizedDbsnp()).toDF
  val clinvarDf: DataFrame = Seq(NormalizedClinvar(chromosome = "1", start = 69897, reference = "T", alternate = "C")).toDF
  val genesDf: DataFrame = Seq(EnrichedGenes()).toDF()
  val cosmicDf: DataFrame = Seq(NormalizedCosmicMutationSet(chromosome = "1", start = 69897, reference = "T", alternate = "C")).toDF()

  val etl = Variants(TestETLContext(), snvDatasetId = snvKeyId, splits = Seq(FrequencySplit("frequency", extraAggregations = Seq(SimpleAggregation(name = "zygosities", c = col("zygosity"))))))

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
    cosmic.id -> cosmicDf
  )

  "transformSingle" should "return expected result" in {
    val df = etl.transformSingle(data)

    val result = df.as[EnrichedVariant].collect()
    result.length shouldBe 1
    result.head shouldBe EnrichedVariant()
  }

  "withCosmic" should "enrich variants with Cosmic data" in {
    val variants = Seq(
      EnrichedVariant(chromosome = "1", start = 1, reference = "A", alternate = "T"), // Unique cosmic info
      EnrichedVariant(chromosome = "2", start = 1, reference = "A", alternate = "T"), // Duplicate cosmic info
      EnrichedVariant(chromosome = "3", start = 1, reference = "A", alternate = "T"), // No cosmic info
    ).toDF().drop("cmc")

    val cosmic = Seq(
      NormalizedCosmicMutationSet(`chromosome` = "1", `start` = 1, `reference` = "A", `alternate` = "T"),
      NormalizedCosmicMutationSet(`chromosome` = "2", `start` = 1, `reference` = "A", `alternate` = "T", `cosmic_sample_mutated` = 1),
      NormalizedCosmicMutationSet(`chromosome` = "2", `start` = 1, `reference` = "A", `alternate` = "T", `cosmic_sample_mutated` = 2), // Should take highest
    ).toDF()

    val result = variants.withCosmic(cosmic)
    result.as[EnrichedVariant].collect() should contain theSameElementsAs Seq(
      EnrichedVariant(chromosome = "1", start = 1, reference = "A", alternate = "T", cmc = CMC()),
      EnrichedVariant(chromosome = "2", start = 1, reference = "A", alternate = "T", cmc = CMC(sample_mutated = 2, sample_ratio = 2.3035901452413586E-5)),
      EnrichedVariant(chromosome = "3", start = 1, reference = "A", alternate = "T", cmc = null),
    )
  }

  "extraAggregations" should "be computed and added to the root of the data" in {
    val job = etl.copy(extraAggregations = Seq(
      collect_set("participant_id") as "participant_ids",
      max("study_id") as "latest_study"
    ))

    val df = job.transformSingle(data)
    val result = df.select("participant_ids", "latest_study").as[(Set[String], String)].collect()
    result.head shouldBe(Set("PA0001", "PA0002"), "S2")
  }
}
