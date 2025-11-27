package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedClinvarVep
import bio.ferlab.datalake.spark3.testmodels.raw.RawClinvarVep
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{TestETLContext, WithSparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}


class ClinvarVepSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_clinvar_vep")

  "ClinvarVepSpec" should "transform ClinvarVep input to ClinvarVep output" in {

    val df = Seq(RawClinvarVep()).toDF()
    val result = new ClinvarVep(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedClinvarVep].collect() should contain theSameElementsAs Seq(NormalizedClinvarVep())

  }

}