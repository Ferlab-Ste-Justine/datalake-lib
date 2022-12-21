package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.{SpliceAiInput, SpliceAiOutput}
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpliceAiSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_spliceai_indel")
  val destination: DatasetConf = conf.getDataset("normalized_spliceai_indel")

  "transform" should "transform SpliceAiIndelInput to SpliceAiIndelOutput" in {
    val inputData = Map(source.id -> Seq(SpliceAiInput("2"), SpliceAiInput("3")).toDF())

    val resultDF = new SpliceAi(variantType = "indel").transformSingle(inputData)

//    ClassGenerator
//      .writeCLassFile(
//        "bio.ferlab.datalake.spark3.testmodels",
//        "SpliceAiOutput",
//        resultDF,
//        "datalake-spark3/src/test/scala/")

    val expectedResults = Seq(SpliceAiOutput("2"), SpliceAiOutput("3"))
    resultDF.as[SpliceAiOutput].collect() should contain allElementsOf expectedResults
  }

}
