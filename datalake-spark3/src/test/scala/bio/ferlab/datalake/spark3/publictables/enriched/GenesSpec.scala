package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.publictables.enriched.Genes._
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.enriched._
import bio.ferlab.datalake.testutils.models.normalized._
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col

class GenesSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val destination: DatasetConf = conf.getDataset("enriched_genes")
  val omim_gene_set: DatasetConf = conf.getDataset("normalized_omim_gene_set")
  val orphanet_gene_set: DatasetConf = conf.getDataset("normalized_orphanet_gene_set")
  val hpo_gene_set: DatasetConf = conf.getDataset("normalized_hpo_gene_set")
  val human_genes: DatasetConf = conf.getDataset("normalized_human_genes")
  val ddd_gene_set: DatasetConf = conf.getDataset("normalized_ddd_gene_set")
  val cosmic_gene_set: DatasetConf = conf.getDataset("normalized_cosmic_gene_set")
  val gnomad_constraint: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")
  val spliceai: DatasetConf = conf.getDataset("enriched_spliceai")

  private val inputData = Map(
    omim_gene_set.id -> Seq(
      NormalizedOmimGeneSet(omim_gene_id = 601013),
      NormalizedOmimGeneSet(omim_gene_id = 601013, phenotype = PHENOTYPE(null, null, null, null))).toDF(),
    orphanet_gene_set.id -> Seq(NormalizedOrphanetGeneSet(gene_symbol = "OR4F5")).toDF(),
    hpo_gene_set.id -> Seq(NormalizedHpoGeneSet()).toDF(),
    human_genes.id -> Seq(NormalizedHumanGenes(), NormalizedHumanGenes(`symbol` = "OR4F4")).toDF(),
    ddd_gene_set.id -> Seq(NormalizedDddGeneCensus(`symbol` = "OR4F5")).toDF(),
    cosmic_gene_set.id -> Seq(NormalizedCosmicGeneSet(`symbol` = "OR4F5")).toDF,
    gnomad_constraint.id -> Seq(
      NormalizedGnomadConstraint(chromosome = "1", start = 69897, symbol = "OR4F5", `pLI` = 1.0f, oe_lof_upper = 0.01f),
      NormalizedGnomadConstraint(chromosome = "1", start = 69900, symbol = "OR4F5", `pLI` = 0.9f, oe_lof_upper = 0.054f)
    ).toDF(),
    spliceai.id -> Seq(EnrichedSpliceAi(`symbol` = "OR4F5")).toDF()
  )

  val job = new Genes(TestETLContext())

  override val dbToCreate: List[String] = List(destination.table.map(_.database).getOrElse("variant"))
  override val dsToClean: List[DatasetConf] = List(destination)

  it should "transform data into genes table" in {

    val resultDF = job.transform(inputData)

    val expectedOrphanet = List(ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive")))
    val expectedOmim = List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("Autosomal dominant"), List("AD")))
    val expectedCosmic = List(COSMIC(List("medullary thyroid", "papillary thyroid", "pheochromocytoma")))

    resultDF(destination.id).where("symbol='OR4F5'").as[EnrichedGenes].collect().head shouldBe
      EnrichedGenes(`orphanet` = expectedOrphanet, `omim` = expectedOmim, `cosmic` = expectedCosmic)

    resultDF(destination.id)
      .where("symbol='OR4F4'")
      .select(
        functions.size(col("orphanet")),
        functions.size(col("ddd")),
        functions.size(col("cosmic"))).as[(Long, Long, Long)].collect().head shouldBe(0, 0, 0)

  }

  it should "write data into genes table" in {

    job.transform(inputData)
    job.load(job.transform(inputData))


    val resultDF = destination.read

    val expectedOrphanet = List(ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive")))
    val expectedOmim = List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("Autosomal dominant"), List("AD")))
    val expectedCosmic = List(COSMIC(List("medullary thyroid", "papillary thyroid", "pheochromocytoma")))

    resultDF.where("symbol='OR4F5'").as[EnrichedGenes].collect().head shouldBe
      EnrichedGenes(`orphanet` = expectedOrphanet, `omim` = expectedOmim, `cosmic` = expectedCosmic)
  }

  "withSpliceAi" should "enrich genes with SpliceAi scores" in {
    val genes = Seq(
      EnrichedGenes(`chromosome` = "1", `symbol` = "gene1"),
      EnrichedGenes(`chromosome` = "1", `symbol` = "gene2"),
      EnrichedGenes(`chromosome` = "2", `symbol` = "gene3"),
      EnrichedGenes(`chromosome` = "3", `symbol` = "gene4"),
    ).toDF().drop("spliceai")

    val spliceai = Seq(
      // snv
      EnrichedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `max_score` = MAX_SCORE(`ds` = 2.0, `type` = Seq("AL"))),
      EnrichedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene2", `max_score` = MAX_SCORE(`ds` = 0.0, `type` = Seq("AG", "AL", "DG", "DL"))),

      // indel
      EnrichedSpliceAi(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "AT", `symbol` = "gene3", `max_score` = MAX_SCORE(`ds` = 1.0, `type` = Seq("AG", "AL")))
    ).toDF()

    val result = genes.withSpliceAi(spliceai)

    val expected = Seq(
      EnrichedGenes(`chromosome` = "1", `symbol` = "gene1", `spliceai` = Some(SPLICEAI(`ds` = 2.0, `type` = Some(List("AL"))))),
      EnrichedGenes(`chromosome` = "1", `symbol` = "gene2", `spliceai` = Some(SPLICEAI(`ds` = 0.0, `type` = None))),
      EnrichedGenes(`chromosome` = "2", `symbol` = "gene3", `spliceai` = Some(SPLICEAI(`ds` = 1.0, `type` = Some(List("AG", "AL"))))),
      EnrichedGenes(`chromosome` = "3", `symbol` = "gene4", `spliceai` = None),
    )

    result
      .as[EnrichedGenes]
      .collect() should contain theSameElementsAs expected
  }
}

