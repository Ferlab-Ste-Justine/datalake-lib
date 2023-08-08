package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.testmodels.enriched.{EnrichedConsequences, EnrichedDbnsfp, EnrichedGenes}
import bio.ferlab.datalake.spark3.testmodels.normalized.{NormalizedConsequences, NormalizedEnsemblMapping}
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import java.io.File

class EnrichConsequencesSpec extends SparkSpec with WithTestConfig with BeforeAndAfterAll with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("enriched_dbnsfp")
  val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")
  import spark.implicits._
  private val data = Map(
    normalized_consequences.id -> Seq(NormalizedConsequences()).toDF(),
    dbnsfp_original.id -> Seq(EnrichedDbnsfp()).toDF,
    normalized_ensembl_mapping.id -> Seq(NormalizedEnsemblMapping(`ensembl_gene_id` = "ENSG00000186092", `ensembl_transcript_id` = "ENST00000335137")).toDF,
    enriched_genes.id -> Seq(EnrichedGenes()).toDF,
  )

  val etl = new Consequences(TestETLContext(RunStep.default_load))

  override val dbToCreate: List[String] = List("variant")
  override val dsToClean: List[DatasetConf] = List(enriched_consequences)

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(new File("spark-warehouse"))

    data.foreach { case (id, df) =>
      val ds = conf.getDataset(id)

      LoadResolver
        .write(spark, conf)(ds.format, LoadType.OverWrite)
        .apply(ds, df)
    }
  }

  "consequences job" should "transform data in expected format" in {
    val resultDf = etl.transformSingle(data)
    val result = resultDf.as[EnrichedConsequences].collect().head

    //    ClassGenerator.writeCLassFile("bio.ferlab.clin.model", "EnrichedConsequences", resultDf, "src/test/scala/")

    result shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
  }

  "consequences job" should "run" in {
    etl.run()

    val result = enriched_consequences.read.as[EnrichedConsequences].collect().head
    result shouldBe EnrichedConsequences(
      `predictions` = null,
      `conservations` = null
    )
  }
}


