package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.config.SourceConf
import bio.ferlab.datalake.spark3.testmodels._
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class importGenesTableSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  val destination       : SourceConf = conf.getSource("genes")
  val omim_gene_set     : SourceConf = conf.getSource("omim_gene_set")
  val orphanet_gene_set : SourceConf = conf.getSource("orphanet_gene_set")
  val hpo_gene_set      : SourceConf = conf.getSource("hpo_gene_set")
  val human_genes       : SourceConf = conf.getSource("human_genes")
  val ddd_gene_set      : SourceConf = conf.getSource("ddd_gene_set")
  val cosmic_gene_set   : SourceConf = conf.getSource("cosmic_gene_set")

  "run" should "creates genes table" in {

    val inputData = Map(
      omim_gene_set     -> Seq(OmimOutput(omim_gene_id = 601013), OmimOutput(omim_gene_id = 601013, phenotype = PHENOTYPE(null, null, null, null))).toDF(),
      orphanet_gene_set -> Seq(OrphanetOutput(gene_symbol = "OR4F5")).toDF(),
      hpo_gene_set      -> Seq(HpoGeneSetOutput()).toDF(),
      human_genes       -> Seq(HumanGenesOutput(), HumanGenesOutput(`symbol` = "OR4F4")).toDF(),
      ddd_gene_set      -> Seq(DddGeneCensusOutput(`symbol` = "OR4F5")).toDF(),
      cosmic_gene_set   -> Seq(CosmicCancerGeneCensusOutput(`symbol` = "OR4F5")).toDF
    )

    val resultDF = new ImportGenesTable().transform(inputData)

    val expectedOrphanet = List(ORPHANET(17601, "Multiple epiphyseal dysplasia, Al-Gazali type", List("Autosomal recessive")))
    val expectedOmim = List(OMIM("Shprintzen-Goldberg syndrome", "182212", List("Autosomal dominant"), List("AD")))

    resultDF.show(false)

    resultDF.where("symbol='OR4F5'").as[GenesOutput].collect().head shouldBe
      GenesOutput(`orphanet` = expectedOrphanet, `omim` = expectedOmim)

    resultDF
      .where("symbol='OR4F4'")
      .select(
        functions.size(col("orphanet")),
        functions.size(col("ddd")),
        functions.size(col("cosmic"))).as[(Long, Long, Long)].collect().head shouldBe (0, 0, 0)

  }

}

