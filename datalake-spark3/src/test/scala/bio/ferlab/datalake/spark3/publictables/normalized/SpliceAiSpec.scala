package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.normalized.NormalizedSpliceAi
import bio.ferlab.datalake.testutils.models.raw.RawSpliceAi
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class SpliceAiSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_spliceai_indel")
  val destination: DatasetConf = conf.getDataset("normalized_spliceai_indel")

  "transform" should "transform RawSpliceAi to NormalizedSpliceAi" in {
    val inputData = Map(source.id -> Seq(RawSpliceAi("2"), RawSpliceAi("3")).toDF())

    val resultDF = new SpliceAi(TestETLContext(), variantType = "indel").transformSingle(inputData)

    //    ClassGenerator
    //      .writeCLassFile(
    //        "bio.ferlab.datalake.testutils.models.normalized",
    //        "NormalizedSpliceAi",
    //        resultDF,
    //        "datalake-spark3/src/test/scala/")

    val expectedResults = Seq(NormalizedSpliceAi("2"), NormalizedSpliceAi("3"))
    resultDF.as[NormalizedSpliceAi].collect() shouldBe expectedResults
  }

}
