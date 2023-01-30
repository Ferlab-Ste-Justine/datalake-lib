package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.publictables.normalized.gnomad.GnomadConstraint
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedGnomadConstraint
import bio.ferlab.datalake.spark3.testmodels.raw.RawGnomadConstraint
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GnomadConstraintSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_gnomad_constraint_v2_1_1")
  val destination: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")

  "transform" should "transform RawGnomadConstraint to NormalizedGnomadConstraint" in {
    val inputData = Map(source.id -> Seq(RawGnomadConstraint()).toDF())

    val resultDF = new GnomadConstraint().transformSingle(inputData)

//    ClassGenerator
//      .writeCLassFile(
//        "bio.ferlab.datalake.spark3.testmodels.normalized",
//        "NormalizedGnomadConstraint",
//        resultDF,
//        "datalake-spark3/src/test/scala/")

    val expectedResults = Seq(NormalizedGnomadConstraint())
    resultDF.as[NormalizedGnomadConstraint].collect() shouldBe expectedResults
  }

}
