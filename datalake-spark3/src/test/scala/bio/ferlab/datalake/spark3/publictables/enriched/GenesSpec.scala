package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
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
    ).toDF()
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
}

