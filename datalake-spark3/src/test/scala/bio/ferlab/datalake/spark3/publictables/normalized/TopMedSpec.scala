package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.normalized.NormalizedTopmed
import bio.ferlab.datalake.testutils.models.raw.{RawTopMedFreeze8, RawTopMedFreeze10}
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class TopMedSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_topmed_bravo")
  val destination: DatasetConf = conf.getDataset("normalized_topmed_bravo")

  "transform freeze_8" should "transform TOPMed freeze_8 input to TOPMed output" in {
    val df = Seq(RawTopMedFreeze8()).toDF()

    val result = TopMed(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedTopmed].collect() should contain theSameElementsAs Seq(NormalizedTopmed())
  }

  "transform freeze_10" should "transform TOPMed freeze_10 input to TOPMed output" in {
    val df = Seq(RawTopMedFreeze10()).toDF()

    val result = TopMed(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedTopmed].collect() should contain theSameElementsAs Seq(NormalizedTopmed(name=None))
  }
}



