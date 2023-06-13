package bio.ferlab.datalake.spark3.genomics

import bio.ferlab.datalake.spark3.genomics.Frequencies._
import bio.ferlab.datalake.spark3.testmodels.frequency.{VariantFrequencyOutputByStudy, VariantFrequencyOutputByStudyAffected}
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedSNV
import bio.ferlab.datalake.spark3.testutils.{WithSparkSession, WithTestConfig}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FrequenciesSpec extends AnyFlatSpec with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  "frequencies" should "return expected values" in {
    val input = Seq(
      NormalizedSNV(),
      NormalizedSNV(participant_id = "P2", transmission_mode = "AD"),
      NormalizedSNV(study_id = "S2", participant_id = "P3")
    ).toDF()


    val result = input.freq(
      FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id")), extraAggregations = Seq(
          AtLeastNElements(name = "participant_ids", c = col("participant_id"), n = 2),
          SimpleAggregation(name = "transmissions", c = col("transmission_mode"))
        )
      ),
      FrequencySplit("frequency_kf")
    )

    val collectedResult = result.as[VariantFrequencyOutputByStudy].collect()
    collectedResult.length shouldBe 1
    collectedResult.head shouldBe VariantFrequencyOutputByStudy()

  }

  it should "return expected values with affected parameter set to true" in {
    val input = Seq(
      NormalizedSNV(),
      NormalizedSNV(participant_id = "P2", affected_status = true, transmission_mode = "AD"),
      NormalizedSNV(study_id = "S2", participant_id = "P3")
    ).toDF()
    input.show(false)
    val result = input.freq(
      FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id")), byAffected = true, extraAggregations = Seq(
          AtLeastNElements(name = "participant_ids", c = col("participant_id"), n = 2),
          SimpleAggregation(name = "transmissions", c = col("transmission_mode"))
        )
      ),
      FrequencySplit("frequency_kf", byAffected = true)
    )

    val collectedResult = result.as[VariantFrequencyOutputByStudyAffected].collect()
    collectedResult.length shouldBe 1
    collectedResult.head shouldBe VariantFrequencyOutputByStudyAffected()

  }

}
