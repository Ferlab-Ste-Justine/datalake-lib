package bio.ferlab.datalake.spark3.genomics

import bio.ferlab.datalake.spark3.genomics.Frequencies._
import bio.ferlab.datalake.spark3.testmodels.frequency.{VariantFrequencyInput, VariantFrequencyOutputByStudy, VariantFrequencyOutputByStudyAffected}
import bio.ferlab.datalake.spark3.testutils.{WithSparkSession, WithTestConfig}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FrequenciesSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  "frequencies" should "return expected values" in {
    val input = Seq(
      VariantFrequencyInput(),
      VariantFrequencyInput(),
      VariantFrequencyInput(study_id = "S2")
    ).toDF()


    val result = input.freq(
      FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id"))),
      FrequencySplit("frequency_kf"),
    )

    val collectedResult = result.as[VariantFrequencyOutputByStudy].collect()
    collectedResult.length shouldBe 1
    collectedResult.head shouldBe VariantFrequencyOutputByStudy()

  }

  it should "return expected values with affected parameter set to true" in {
    val input = Seq(
      VariantFrequencyInput(),
      VariantFrequencyInput(affected_status = true),
      VariantFrequencyInput(study_id = "S2")
    ).toDF()

    val result = input.freq(
      FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id")), byAffected = true),
      FrequencySplit("frequency_kf", byAffected = true),
    )

    val collectedResult = result.as[VariantFrequencyOutputByStudyAffected].collect()
    collectedResult.length shouldBe 1
    collectedResult.head shouldBe VariantFrequencyOutputByStudyAffected()

  }

}
