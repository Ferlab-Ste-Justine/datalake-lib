package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.testmodels.{ClinvarInput, ClinvarOutput}
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportClinvarSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {
  import spark.implicits._

  val clinvar_vcf = conf.getDataset("clinvar_vcf")

  "run" should "creates clinvar table" in {

    withOutputFolder("output") { _ =>
      val inputData = Map(clinvar_vcf.id -> Seq(ClinvarInput()).toDF())

      val resultDF = new ImportClinvar().transform(inputData)

      val expectedResult = ClinvarOutput()

      resultDF.as[ClinvarOutput].collect().head shouldBe expectedResult

    }
  }

}

