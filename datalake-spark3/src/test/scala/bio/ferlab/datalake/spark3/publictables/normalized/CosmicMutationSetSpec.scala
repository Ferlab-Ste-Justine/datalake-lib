package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.publictables.normalized.cosmic.CosmicMutationSet
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.normalized.NormalizedCosmicMutationSet
import bio.ferlab.datalake.testutils.models.raw.RawCosmicMutationSet
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class CosmicMutationSetSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_cosmic_mutation_set")
  val destination: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")

  it should "normalize cosmic mutation set" in {
    val df = Seq(RawCosmicMutationSet()).toDF()

    val result = CosmicMutationSet(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedCosmicMutationSet].collect() should contain theSameElementsAs Seq(NormalizedCosmicMutationSet())
  }
}



