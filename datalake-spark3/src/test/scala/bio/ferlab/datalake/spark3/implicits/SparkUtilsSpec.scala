package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.SparkUtils._
import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.datalake.testutils.models.Genotype
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkUtilsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  val hom_00: Genotype = Genotype(Array(0, 0))
  val hom_11: Genotype = Genotype(Array(1, 1))
  val het_01: Genotype = Genotype(Array(0, 1))
  val het_10: Genotype = Genotype(Array(1, 0))

  val unk: Genotype = Genotype(Array(-1, 0))

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

  private val schemaNestedFieldExists = StructType(Seq(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("address", StructType(Seq(
      StructField("street", StringType),
      StructField("city", StringType),
      StructField("state", StringType)
    )))
  ))

  "isNestedFieldExists" should "return true when the nested field exists in the schema" in {
    val result = isNestedFieldExists(schemaNestedFieldExists, "address.street")
    result should be(true)
  }

  it should "return false when the nested field does not exist in the schema" in {
    val result = isNestedFieldExists(schemaNestedFieldExists, "address.zip")
    result should be(false)
  }

  it should "return false when the field is not a nested field" in {
    val result = isNestedFieldExists(schemaNestedFieldExists, "name.first")
    result should be(false)
  }

  "array_remove_empty" should "return an array without empty values" in {
    val df = Seq(
      (Seq("a", "b", "c", "d")),
      (Seq("a", "", "c", "d")),
      (Seq("a", null, "c", "d")),
      (Seq("a", "b", "", "d")),
      (Seq("a", "b", "c", ""))
    ).toDF("array")

    val res = df.select(array_remove_empty($"array") as "array")
    res.as[Seq[String]].collect() should contain theSameElementsAs Seq(
      Seq("a", "b", "c", "d"),
      Seq("a", "c", "d"),
      Seq("a", "c", "d"),
      Seq("a", "b", "d"),
      Seq("a", "b", "c")
    )
  }
}

