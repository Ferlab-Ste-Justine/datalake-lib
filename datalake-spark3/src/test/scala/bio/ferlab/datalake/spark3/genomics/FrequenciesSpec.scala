package bio.ferlab.datalake.spark3.genomics

import bio.ferlab.datalake.spark3.genomics.Frequencies._
import bio.ferlab.datalake.spark3.testmodels.frequency._
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedSNV
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.spark.sql.functions.col

class FrequenciesSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  "frequencies" should "return expected values" in {
    val input = Seq(
      NormalizedSNV(),
      NormalizedSNV(participant_id = "P2", transmission_mode = "AD", zygosity = "HET", calls = Seq(0, 1)),
      NormalizedSNV(study_id = "S2", participant_id = "P3", study_code = "STUDY_CODE_2"),
      NormalizedSNV(chromosome = "2", study_id = "S2", participant_id = "P4", study_code = "STUDY_CODE_2")
    ).toDF()


    val result = input.freq(split = Seq(
      FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id")), extraAggregations = Seq(
        AtLeastNElements(name = "participant_ids", c = col("participant_id"), n = 2),
        SimpleAggregation(name = "transmissions", c = col("transmission_mode")),
        FirstElement(name = "study_code", col("study_code"))
      )
      ),
      FrequencySplit("frequency_kf", extraAggregations = Seq(SimpleAggregation(name = "zygosities", c = col("zygosity"))))
    )
    )

    result.show(false)
    val collectedResult = result.as[VariantFrequencyOutputByStudy].collect()
    collectedResult.length shouldBe 2
    collectedResult.find(_.chromosome == "1") shouldBe Some(VariantFrequencyOutputByStudy())
    collectedResult.find(_.chromosome == "2") shouldBe Some(VariantFrequencyOutputByStudy(
      chromosome = "2",
      frequency_kf = GlobalFrequency(total = Frequency(ac = 2, pc = 1, hom = 1, an = 8, pn = 4, af = 0.25, pf = 0.25), zygosities = Set("HOM")),
      frequency_by_study_id = Set(
        FrequencyByStudyId(study_id = "S2", total = Frequency(ac = 2, pc = 1, hom = 1, an = 4, pn = 2, af = 0.5, pf = 0.5), participant_ids = null, transmissions = Set("AR"), study_code = "STUDY_CODE_2"))
    ))

  }

  it should "return expected values with affected parameter set to true" in {
    val input = Seq(
      NormalizedSNV(),
      NormalizedSNV(participant_id = "P2", affected_status = true, transmission_mode = "AD", zygosity = "HET", calls = Seq(0, 1)),
      NormalizedSNV(study_id = "S2", participant_id = "P3", study_code = "STUDY_CODE_2"),
      NormalizedSNV(chromosome = "2", study_id = "S2", participant_id = "P4", study_code = "STUDY_CODE_2")
    ).toDF()
    input.show(false)
    val result = input.freq(split = Seq(
      FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id")), byAffected = true, extraAggregations = Seq(
        AtLeastNElements(name = "participant_ids", c = col("participant_id"), n = 2),
        SimpleAggregation(name = "transmissions", c = col("transmission_mode")),
        FirstElement(name = "study_code", col("study_code"))
      )
      ),
      FrequencySplit("frequency_kf", byAffected = true, extraAggregations = Seq(SimpleAggregation(name = "zygosities", c = col("zygosity"))))
    ))
    result.show(false)
    result.printSchema()

    val collectedResult = result.as[VariantFrequencyOutputByStudyAffected].collect()
    collectedResult.length shouldBe 2

    collectedResult.find(_.chromosome == "1") shouldBe defined
    val chr1Result = collectedResult.find(_.chromosome == "1").get
    val chr1Expected = VariantFrequencyOutputByStudyAffected()
    chr1Result.frequency_kf shouldBe chr1Expected.frequency_kf
    chr1Result.frequency_by_study_id.size shouldBe 2
    chr1Result.frequency_by_study_id.find(_.study_id == "S1") shouldBe chr1Expected.frequency_by_study_id.find(_.study_id == "S1")
    chr1Result.frequency_by_study_id.find(_.study_id == "S2") shouldBe chr1Expected.frequency_by_study_id.find(_.study_id == "S2")

    collectedResult.find(_.chromosome == "2") shouldBe defined
    val chr2Result = collectedResult.find(_.chromosome == "2").get
    val chr2Expected = VariantFrequencyOutputByStudyAffected(
      chromosome = "2",
      frequency_kf = GlobalFrequencyAffected(
        total = Frequency(ac = 2, pc = 1, hom = 1, an = 8, pn = 4, af = 0.25, pf = 0.25),
        affected = Frequency(ac = 0, pc = 0, hom = 0, an = 2, pn = 1, af = 0.0, pf = 0.0),
        not_affected = Frequency(ac = 2, pc = 1, hom = 1, an = 6, pn = 3, af = 2d / 6d, pf = 1d / 3d),
        zygosities = Set("HOM")
      ),
      frequency_by_study_id = Set(
        FrequencyByStudyIdAffected(study_id = "S2",
          total = Frequency(ac = 2, pc = 1, hom = 1, an = 4, pn = 2, af = 0.5, pf = 0.5),
          not_affected = Frequency(ac = 2, pc = 1, hom = 1, an = 4, pn = 2, af = 0.5, pf = 0.5),
          affected = Frequency(0, 0, 0, 0, 0, 0.0, 0.0),
          participant_ids = null,
          transmissions = Set("AR"),
          study_code = "STUDY_CODE_2"
        ),

      )
    )
    chr2Result.frequency_kf shouldBe chr2Expected.frequency_kf
    chr2Result.frequency_by_study_id.size shouldBe 1
    chr2Result.frequency_by_study_id.find(_.study_id == "S2") shouldBe chr2Expected.frequency_by_study_id.find(_.study_id == "S2")


  }

}
