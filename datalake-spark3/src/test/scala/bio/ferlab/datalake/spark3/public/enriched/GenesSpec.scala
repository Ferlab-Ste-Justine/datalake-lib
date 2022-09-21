package bio.ferlab.datalake.spark3.public.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.testmodels._
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GenesSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  spark.sql("CREATE DATABASE IF NOT EXISTS variant")

  val destination      : DatasetConf = conf.getDataset("enriched_genes")
  val omim_gene_set    : DatasetConf = conf.getDataset("normalized_omim_gene_set")
  val orphanet_gene_set: DatasetConf = conf.getDataset("normalized_orphanet_gene_set")
  val hpo_gene_set     : DatasetConf = conf.getDataset("normalized_hpo_gene_set")
  val human_genes      : DatasetConf = conf.getDataset("normalized_human_genes")
  val ddd_gene_set     : DatasetConf = conf.getDataset("normalized_ddd_gene_set")
  val cosmic_gene_set  : DatasetConf = conf.getDataset("normalized_cosmic_gene_set")

  val inputData = Map(
    omim_gene_set.id     -> Seq(
      OmimGeneSetOutput(omim_gene_id = 601013),
      OmimGeneSetOutput(omim_gene_id = 601013, phenotype = PHENOTYPE(null, null, null, null))).toDF(),
    orphanet_gene_set.id -> Seq(OrphanetGeneSetOutput(gene_symbol = "OR4F5")).toDF(),
    hpo_gene_set.id      -> Seq(HpoGeneSetOutput()).toDF(),
    human_genes.id       -> Seq(HumanGenesOutput(), HumanGenesOutput(`symbol` = "OR4F4")).toDF(),
    ddd_gene_set.id      -> Seq(DddGeneCensusOutput(`symbol` = "OR4F5")).toDF(),
    cosmic_gene_set.id   -> Seq(CosmicGeneSetOutput(`symbol` = "OR4F5")).toDF
  )

  val job = new Genes()

  it should "transform data into genes table" in {

    val resultDF = job.transform(inputData)

    val expectedOrphanet = List(ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive")))
    val expectedOmim = List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("Autosomal dominant"), List("AD")))

    resultDF(destination.id).where("symbol='OR4F5'").as[GenesOutput].collect().head shouldBe
      GenesOutput(`orphanet` = expectedOrphanet, `omim` = expectedOmim)

    resultDF(destination.id)
      .where("symbol='OR4F4'")
      .select(
        functions.size(col("orphanet")),
        functions.size(col("ddd")),
        functions.size(col("cosmic"))).as[(Long, Long, Long)].collect().head shouldBe (0, 0, 0)

  }

  it should "write data into genes table" in {

    job.transform(inputData)
    job.load(job.transform(inputData))


    val resultDF = destination.read

    val expectedOrphanet = List(ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive")))
    val expectedOmim = List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("Autosomal dominant"), List("AD")))

    resultDF.show(false)

    resultDF.where("symbol='OR4F5'").as[GenesOutput].collect().head shouldBe
      GenesOutput(`orphanet` = expectedOrphanet, `omim` = expectedOmim)
  }

}

