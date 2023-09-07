package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.publictables.normalized.cosmic.CosmicGeneSet
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.normalized.NormalizedCosmicGeneSet
import bio.ferlab.datalake.testutils.models.raw.RawCosmicGeneSet
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec, TestETLContext}

class CosmicGeneSetSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_cosmic_gene_set")
  val destination: DatasetConf = conf.getDataset("normalized_cosmic_gene_set")

  "transform" should "transform Cosmic input to Cosmic output" in {
    val df = Seq(RawCosmicGeneSet()).toDF()

    val result = CosmicGeneSet(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedCosmicGeneSet].collect() should contain theSameElementsAs Seq(NormalizedCosmicGeneSet())
  }
}



