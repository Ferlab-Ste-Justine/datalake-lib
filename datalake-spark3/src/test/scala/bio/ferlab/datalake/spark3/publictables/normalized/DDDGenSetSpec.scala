package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.normalized.{NormalizedCosmicGeneSet, NormalizedDddGeneCensus}
import bio.ferlab.datalake.testutils.models.raw.RawDDDGeneSet
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class DDDGenSetSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_ddd_gene_set")
  val destination: DatasetConf = conf.getDataset("normalized_ddd_gene_set")

  "transform" should "transform DDD Gene input to DDD Gene output" in {
    val df = Seq(RawDDDGeneSet()).toDF()

    val result = DDDGeneSet(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedDddGeneCensus].collect() should contain theSameElementsAs Seq(NormalizedDddGeneCensus())
  }
}



