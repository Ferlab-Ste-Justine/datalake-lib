package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.genomics.enriched.Variants.DataFrameOps
import bio.ferlab.datalake.spark3.genomics.{FrequencySplit, SimpleAggregation}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.enriched.EnrichedVariant.{CMC, GENES, SPLICEAI}
import bio.ferlab.datalake.testutils.models.enriched.{EnrichedGenes, EnrichedSpliceAi, EnrichedVariant, MAX_SCORE}
import bio.ferlab.datalake.testutils.models.normalized._
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_set, max, transform}

class EnrichedVariantsSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val snvKeyId = "snv"
  val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  val gnomad_constraint: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")
  val gnomad_genomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v2_1_1")
  val gnomad_exomes_v2_1_1: DatasetConf = conf.getDataset("normalized_gnomad_exomes_v2_1_1")
  val gnomad_genomes_v3: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")
  val gnomad_joint_v4: DatasetConf = conf.getDataset("normalized_gnomad_joint_v4")
  val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val genes: DatasetConf = conf.getDataset("enriched_genes")
  val cosmic: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")
  val spliceai_snv: DatasetConf = conf.getDataset("enriched_spliceai_snv")
  val spliceai_indel: DatasetConf = conf.getDataset("enriched_spliceai_indel")

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
  val gnomad_joint_4Df: DataFrame = Seq(NormalizedGnomadJoint4()).toDF
  val dbsnpDf: DataFrame = Seq(NormalizedDbsnp()).toDF
  val clinvarDf: DataFrame = Seq(NormalizedClinvar(chromosome = "1", start = 69897, reference = "T", alternate = "C")).toDF
  val genesDf: DataFrame = Seq(EnrichedGenes()).toDF()
  val cosmicDf: DataFrame = Seq(NormalizedCosmicMutationSet(chromosome = "1", start = 69897, reference = "T", alternate = "C")).toDF()
  val spliceAiSnvDf: DataFrame = Seq(EnrichedSpliceAi(chromosome = "1", start = 69897, reference = "T", alternate = "C", symbol = "OR4F5", ds_ag = 0.01, `max_score` = MAX_SCORE(ds = 0.1, `type` = Some(Seq("AG", "AL", "DG", "DL"))))).toDF()
  val spliceAiIndelDf: DataFrame = Seq(EnrichedSpliceAi(chromosome = "1", start = 69897, reference = "TTG", alternate = "C", symbol = "OR4F5", ds_ag = 0.01, `max_score` = MAX_SCORE(ds = 0.2, `type` = Some(Seq("AG"))))).toDF()

  val etl = Variants(TestETLContext(), snvDatasetId = snvKeyId, splits = Seq(FrequencySplit("frequency", extraAggregations = Seq(SimpleAggregation(name = "zygosities", c = col("zygosity"))))))

  private val data = Map(
    snvKeyId -> occurrencesDf,
    thousand_genomes.id -> genomesDf,
    topmed_bravo.id -> topmed_bravoDf,
    gnomad_genomes_v2_1_1.id -> gnomad_genomes_2_1_1Df,
    gnomad_exomes_v2_1_1.id -> gnomad_exomes_2_1_1Df,
    gnomad_genomes_v3.id -> gnomad_genomes_3Df,
    gnomad_joint_v4.id -> gnomad_joint_4Df,
    dbsnp.id -> dbsnpDf,
    clinvar.id -> clinvarDf,
    genes.id -> genesDf,
    cosmic.id -> cosmicDf,
    spliceai_snv.id -> spliceAiSnvDf,
    spliceai_indel.id -> spliceAiIndelDf
  )

  it should "not join with SpliceAI if it is set to false" in {
    val noSpliceAiETL = etl.copy(spliceAi = false)

    val result = noSpliceAiETL.transformSingle(data).cache()

    result
      .as[EnrichedVariant]
      .collect() should contain theSameElementsAs Seq(
      EnrichedVariant(genes = List(GENES(spliceai = None)),
        gene_external_reference = List("HPO", "Orphanet", "OMIM", "DDD", "Cosmic", "gnomAD"))
    )
  }

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

  "withSpliceAi" should "enrich variants with SpliceAi scores" in {
    val variants = Seq(
      EnrichedVariant(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C"  , variant_class = "SNV", `genes` = List(GENES(`symbol` = Some("gene1")), GENES(`symbol` = Some("gene2")))),
      EnrichedVariant(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "AT" , variant_class = "Insertion"),
      EnrichedVariant(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T"  , variant_class = "SNV"),
      EnrichedVariant(`chromosome` = "3", `start` = 1, `end` = 2, `reference` = "C", `alternate` = "A"  , variant_class = "SNV" , genes = List(null)),
    ).toDF()

    // Remove spliceai nested field from variants df
    val variantsWithoutSpliceAi = variants.withColumn("genes", transform($"genes", g => g.dropFields("spliceai")))

    val spliceAiSnv = Seq(
      // snv
      EnrichedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `max_score` = MAX_SCORE(`ds` = 2.0, `type` = Some(Seq("AL")))),
      EnrichedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene2", `max_score` = MAX_SCORE(`ds` = 0.0, `type` = None)),
      EnrichedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene3", `max_score` = MAX_SCORE(`ds` = 0.0, `type` = None)),
    ).toDF()

    val spliceAiIndel = Seq(
      // indel
      EnrichedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "AT", `symbol` = "OR4F5", `max_score` = MAX_SCORE(`ds` = 1.0, `type` = Some(Seq("AG", "AL"))))
    ).toDF()

    val result = variantsWithoutSpliceAi.withSpliceAi(spliceAiSnv, spliceAiIndel)

    val expected = Seq(
      EnrichedVariant(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", variant_class = "SNV", `genes` = List(
        GENES(`symbol` = Some("gene1"), `spliceai` = Some(SPLICEAI(`ds` = 2.0, `type` = Some(Seq("AL"))))),
        GENES(`symbol` = Some("gene2"), `spliceai` = Some(SPLICEAI(`ds` = 0.0, `type` = None))),
      )),
      EnrichedVariant(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "AT", variant_class = "Insertion", `genes` = List(GENES(`spliceai` = Some(SPLICEAI(`ds` = 1.0, `type` = Some(Seq("AG", "AL"))))))),
      EnrichedVariant(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "A", `alternate` = "T", variant_class = "SNV"       , `genes` = List(GENES(`spliceai` = None))),
      EnrichedVariant(`chromosome` = "3", `start` = 1, `end` = 2, `reference` = "C", `alternate` = "A", variant_class = "SNV"       , `genes` = List(null))
    ).toDF().selectLocus($"genes.spliceai").collect()

    result
      .selectLocus($"genes.spliceai")
      .collect() should contain theSameElementsAs expected
  }
}
