package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.SparkUtils._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.columns._
import bio.ferlab.datalake.spark3.testmodels.Genotype
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

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

  "filename" should "return name of the input files" in {
    val df = spark.read.json(getClass.getResource("/filename").getFile).select($"id", filename)
    df.as[(String, String)].collect() should contain theSameElementsAs Seq(
      ("1", "file1.json"),
      ("2", "file1.json"),
      ("3", "file2.json")
    )
  }

  "union" should "return a unioned df if both df are not empty" in {
    val df1 = Seq("1", "2").toDF("a")
    val df2 = Seq("3", "4").toDF("a")

    union(df1, df2).select("a").as[String].collect() should contain theSameElementsAs Seq(
      "1",
      "2",
      "3",
      "4"
    )
  }

  it should "return df1 if df2 is empty" in {
    val df1 = Seq("1", "2").toDF("a")
    val df2 = spark.emptyDataFrame

    union(df1, df2).select("a").as[String].collect() should contain theSameElementsAs Seq("1", "2")
  }

  it should "return df2 if df1 is empty" in {
    val df1 = spark.emptyDataFrame
    val df2 = Seq("3", "4").toDF("a")
    union(df1, df2).select("a").as[String].collect() should contain theSameElementsAs Seq("3", "4")
  }

  "fileExists" should "return false if there is no file associated to the pattern" in {
    val path = getClass.getResource("/input_vcf/SD_123456").getFile

    val cgp = s"$path/*.unknown"
    println(cgp)
    fileExist(cgp) shouldBe false
  }

  it should "return true if there is files associated to the pattern" in {
    val path = getClass.getResource("/input_vcf/SD_123456").getFile

    val vcf = s"$path/*.CGP.filtered.deNovo.vep.vcf.gz"
    println(vcf)
    fileExist(vcf) shouldBe true
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
      ("1", "Male"  , false, Array(0, 0)  , Array(0, 0), Array(0, 0), false, false, false, "non carrier proband"),
      ("X", "Female", false, Array(0, 0)  , Array(0, 0), Array(0, 0), false, false, false, "non carrier proband"),
      ("1", "Female", false, null         , Array(0, 0), Array(0, 0), false, false, false, "unknown proband genotype"),
      ("X", "Male"  , false, null         , Array(0, 0), Array(0, 0), false, false, false, "unknown proband genotype"),
      ("1", "Female", false, Array(-1, -1), Array(0, 0), Array(0, 0), false, false, false, "unknown proband genotype"),
      ("X", "Male"  , false, Array(-1, -1), Array(0, 0), Array(0, 0), false, false, false, "unknown proband genotype"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(0, 0), true, false, false, "autosomal_dominant (de_novo)"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(0, 1), true, false, true , "autosomal_dominant"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 1), Array(0, 0), true, true , false, "autosomal_dominant"),
      ("1", "Male"  , false, Array(0, 1)  , Array(0, 1), Array(0, 1), true, true , true , "autosomal_dominant"),
      ("1", "Male"  , false, Array(1, 1)  , Array(0, 1), Array(0, 1), true, false, false, "autosomal_recessive"),
      ("1", "Male"  , false, Array(1, 1)  , Array(0, 1), Array(1, 1), true, false, true , "autosomal_recessive"),
      ("1", "Male"  , false, Array(1, 1)  , Array(1, 1), Array(0, 1), true, true , false, "autosomal_recessive"),
      ("1", "Male"  , false, Array(1, 1)  , Array(1, 1), Array(1, 1), true, true , true , "autosomal_recessive"),
      ("X", "Female", false, Array(0, 1)  , Array(0, 0), Array(0, 0), true, false, false, "x_linked_dominant (de_novo)"),
      ("X", "Male"  , false, Array(0, 1)  , Array(0, 0), Array(0, 0), true, false, false, "x_linked_recessive (de_novo)"),
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
      ("X", "Male"  , false, Array(1, 1)  , Array(0, 0), Array(0, 0), true, false, false, "x_linked_recessive (de_novo)"),
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
      .withGenotypeTransmission("transmission", $"father_calls", $"mother_calls")

    result.show(false)
    result
      .where(
        functions.not(col("expectedResult") === col("transmission")) or
          (col("expectedResult").isNotNull and col("transmission").isNull))
      .select("expectedResult", "transmission")
      .as[(String, String)]
      .collect() shouldBe Array.empty[(String, String)] //makes it easy to debug in case the test fails

  }

}

