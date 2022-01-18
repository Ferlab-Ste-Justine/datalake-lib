package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.testmodels.Genotype
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GenomicImplicitsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  val hom_00: Genotype = Genotype(Array(0, 0))
  val hom_11: Genotype = Genotype(Array(1, 1))
  val het_01: Genotype = Genotype(Array(0, 1))
  val het_10: Genotype = Genotype(Array(1, 0))

  val unk: Genotype = Genotype(Array(-1, 0))

  "zygosity" should "return HOM for 1/1" in {
    val df = Seq(hom_11).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "HOM"
  }

  it should "return HET for 0/1" in {
    val df = Seq(het_01).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "HET"
  }
  it should "return HET for 1/0" in {
    val df = Seq(het_10).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "HET"
  }
  it should "return WT for 0/0" in {
    val df = Seq(hom_00).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "WT"
  }

  it should "return UNK otherwise" in {
    val df = Seq(unk).toDF("genotypes")
    df.select(zygosity($"genotypes")).as[String].collect() should contain only "UNK"
  }

  "colFromArrayOrField" should "return first element of an array" in {
    val df = Seq(
      Seq(1)
    ).toDF("first_array")

    val res = df.select(SparkUtils.colFromArrayOrField(df, "first_array") as "first_col")
    res.as[Int].collect() should contain only 1

  }

  it should "return column" in {
    val df = Seq(
      1
    ).toDF("first_col")

    val res = df.select(SparkUtils.colFromArrayOrField(df, "first_col"))

    res.as[Int].collect() should contain only 1

  }

  it should "return parental origins accordingly" in {
    val MTH = "mth"
    val FTH = "fth"

    val input_occurrences = Seq(
      ("Y", true , Array(0, 1), Array(0, 1) , null        , "HET", null),
      ("X", true , Array(0, 1), Array(-1, 1), Array(0, 1) , "HET", null),
      ("1", true , Array(0, 0), Array(0, 1) , Array(0, 1) , "WT" , null),
      ("1", true , Array(0, 1), Array(0, 1) , Array(0, 0) , "HET", FTH),
      ("1", true , Array(0, 1), Array(0, 0) , Array(0, 1) , "HET", MTH),
      ("1", true , Array(0, 1), Array(0, 1) , Array(1, 1) , "HET", MTH),
      ("1", true , Array(0, 1), Array(1, 1) , Array(0, 1) , "HET", FTH),
      ("1", true , Array(0, 1), Array(-1, 1), Array(1, 1) , "HET", MTH),
      ("1", true , Array(0, 1), Array(1, 1) , Array(-1, 1), "HET", FTH),
      ("1", true , Array(0, 1), Array(0, 1) , Array(0, 1) , "HET", null)
    ).toDF("chromosome", "is_multi_allelic", "calls", "father_calls", "mother_calls", "zygosity", "expectedResult")

    val result = input_occurrences.withParentalOrigin("parental_origin", $"father_calls", $"mother_calls", MTH, FTH)

    result.show(false)
    result
      .where(functions.not(col("expectedResult") === col("parental_origin")))
      .select("expectedResult", "parental_origin")
      .as[(String, String)]
      .collect() shouldBe Array.empty[(String, String)]

  }

  it should "return transmissions accordingly" in {

    val input_occurrences = List(
      ("1", "Male"  , false, Array(0, 1)  , null       , null       , true, false, false, "unknown_parents_genotype"),
      ("1", "Male"  , false, Array(0, 1)  , null       , Array(0, 0), true, false, false, "unknown_father_genotype"),
      ("X", "Female", false, Array(0, 1)  , Array(0, 0), null       , true, false, false, "unknown_mother_genotype"),
      ("1", "Male"  , false, Array(1, 1)  , null       , Array(0, 0), true, false, false, "unknown_father_genotype"),
      ("X", "Female", false, Array(1, 1)  , Array(0, 0), null       , true, false, false, "unknown_mother_genotype"),
      ("1", "Male"  , false, Array(0, 0)  , null       , Array(0, 0), false, false, false, null),
      ("1", "Male"  , false, Array(0, 0)  , Array(0, 0), null       , false, false, false, null),
      ("1", "Male"  , false, Array(0, 0)  , Array(0, 0), Array(0, 0), false, false, false, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0)  , Array(0, 0), Array(0, 0), false, false, false, "non_carrier_proband"),
      ("1", "Female", false, null         , Array(0, 0), Array(0, 0), false, false, false, "unknown_proband_genotype"),
      ("X", "Male"  , false, null         , Array(0, 0), Array(0, 0), false, false, false, "unknown_proband_genotype"),
      ("1", "Female", false, Array(-1, -1), Array(0, 0), Array(0, 0), false, false, false, "unknown_proband_genotype"),
      ("X", "Male"  , false, Array(-1, -1), Array(0, 0), Array(0, 0), false, false, false, "unknown_proband_genotype"),
      ("1", "Male"  , false, Array(1, 0)  , Array(0, 0), Array(0, 0), true, false, false, "autosomal_dominant_de_novo"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(0, 0), true, false, false, "autosomal_dominant_de_novo"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(0, 1), true, false, true , "autosomal_dominant"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 1), Array(0, 0), true, true , false, "autosomal_dominant"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 1), Array(0, 1), true, true , true , "autosomal_dominant"),
      ("1", "Male"  , false, Array(1, 1)  , Array(0, 1), Array(0, 1), true, false, false, "autosomal_recessive"),
      ("1", "Male"  , false, Array(1, 1)  , Array(0, 1), Array(1, 1), true, false, true , "autosomal_recessive"),
      ("1", "Male"  , false, Array(1, 1)  , Array(1, 1), Array(0, 1), true, true , false, "autosomal_recessive"),
      ("1", "Male"  , false, Array(1, 1)  , Array(1, 1), Array(1, 1), true, true , true , "autosomal_recessive"),
      ("X", "Female", false, Array(0, 1)  , Array(0, 0), Array(0, 0), true, false, false, "x_linked_dominant_de_novo"),
      ("X", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(0, 0), true, false, false, "x_linked_recessive_de_novo"),
      ("X", "Female", false, Array(0, 1)  , Array(0, 0), Array(0, 1), true, false, true , "x_linked_dominant"),
      ("X", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(0, 1), true, false, false, "x_linked_recessive"),
      ("X", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(1, 1), true, false, true, "x_linked_recessive"),
      ("X", "Female", false, Array(0, 1)  , Array(0, 1), Array(0, 0), true, true , false, "x_linked_dominant"),
      ("X", "Female", false, Array(0, 1)  , Array(0, 1), Array(0, 1), true, true , true , "x_linked_dominant"),
      ("X", "Male"  , false, Array(0, 1)  , Array(0, 1), Array(0, 1), true, true , false, "x_linked_recessive"),
      ("X", "Male"  , false, Array(0, 1)  , Array(0, 1), Array(1, 1), true, true , true , "x_linked_recessive"),
      ("X", "Female", false, Array(0, 1)  , Array(1, 1), Array(0, 0), true, true , false, "x_linked_recessive"),
      ("X", "Female", false, Array(0, 1)  , Array(1, 1), Array(0, 1), true, true , true , "x_linked_dominant"),
      ("X", "Male"  , false, Array(0, 1)  , Array(1, 1), Array(0, 1), true, true , false, "x_linked_recessive"),
      ("X", "Male"  , false, Array(0, 1)  , Array(1, 1), Array(1, 1), true, true , true , "x_linked_recessive"),
      ("X", "Male"  , false, Array(1, 1)  , Array(0, 0), Array(0, 0), true, false, false, "x_linked_recessive_de_novo"),
      ("X", "Male"  , false, Array(1, 1)  , Array(0, 0), Array(0, 1), true, false, false, "x_linked_recessive"),
      ("X", "Male"  , false, Array(1, 1)  , Array(0, 0), Array(1, 1), true, false, true , "x_linked_recessive"),
      ("X", "Female", false, Array(1, 1)  , Array(0, 1), Array(0, 1), true, true , false, "x_linked_recessive"),
      ("X", "Male"  , false, Array(1, 1)  , Array(0, 1), Array(0, 1), true, true , false, "x_linked_recessive"),
      ("X", "Female", false, Array(1, 1)  , Array(0, 1), Array(1, 1), true, true , true , "x_linked_recessive"),
      ("X", "Male"  , false, Array(1, 1)  , Array(0, 1), Array(1, 1), true, true , true , "x_linked_recessive"),
      ("X", "Female", false, Array(1, 1)  , Array(1, 1), Array(0, 1), true, true , false, "x_linked_recessive"),
      ("X", "Male"  , false, Array(1, 1)  , Array(1, 1), Array(0, 1), true, true , false, "x_linked_recessive"),
      ("X", "Female", false, Array(1, 1)  , Array(1, 1), Array(1, 1), true, true , true , "x_linked_recessive"),
      ("X", "Male"  , false, Array(1, 1)  , Array(1, 1), Array(1, 1), true, true , true , "x_linked_recessive"),
    )
      .toDF("chromosome", "gender", "is_multi_allelic", "calls", "father_calls", "mother_calls",
        "affected_status", "father_affected_status", "mother_affected_status", "expectedResult")

    val result = input_occurrences
      .withGenotypeTransmission("transmission",
        "calls",
        "gender",
        "affected_status",
        "father_calls",
        "father_affected_status",
        "mother_calls",
        "mother_affected_status")

    result.show(false)
    result
      .where(
        functions.not(col("expectedResult") === col("transmission")) or
          (col("expectedResult").isNotNull and col("transmission").isNull))
      .select("expectedResult", "transmission")
      .as[(String, String)]
      .collect() shouldBe Array.empty[(String, String)] //makes it easy to debug in case the test fails

  }

  it should "compute transmissions per locus" in {

    val df = Seq(
      ("1-222-A-G", "autosomal_dominant"),
      ("1-222-A-G", "autosomal_dominant"),
      ("2-222-A-G", "autosomal_recessive"),
      ("2-222-A-G", "denovo"),
      ("X-222-A-G", "x_linked_recessive")
    ).toDF("locus", "transmission")

    val resultDf = df.withTransmissionPerLocus(Seq("locus"), "transmission", "result")
    resultDf.show(false)

    resultDf.select("result").as[Map[String, Long]].collect() should contain allElementsOf Seq(
      Map("autosomal_dominant" -> 2),
      Map("autosomal_recessive" -> 1, "denovo" -> 1),
      Map("x_linked_recessive" -> 1)
    )

  }

}

