package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import bio.ferlab.datalake.spark3.transformation
import bio.ferlab.datalake.spark3.transformation.NormalizeColumnName.replace_special_char_by_ansii_code
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.log4j.{Level, Logger}

import java.sql
import java.time.LocalDate
import scala.Seq

class TransformationSpec extends SparkSpec {

  val input: String = getClass.getClassLoader.getResource("filename/").getFile + "file1.json"

  import spark.implicits._

  "Cast" should "cast a list of columns into another type" in {

    val df =
      Seq(("1", "2", "test"), ("3", "4", "test"))
        .toDF("a", "b", "c")
    df.show(false)
    df.printSchema()

    val transformations = List(Cast(IntegerType, "a", "b"))

    Transformation.applyTransformations(df, transformations).as[(Int, Int, String)].collect() should contain allElementsOf
      Seq((1, 2, "test"), (3, 4, "test"))


  }

  "Date" should "cast a strings or a timestamp into a date" in {

    val df =
      Seq((java.sql.Timestamp.valueOf("1900-01-01 12:23:34.1234"), "test"), (java.sql.Timestamp.valueOf("2000-12-31 12:23:34.1234"), "test"))
        .toDF("a", "c")

    val transformations = List(ToDate("", "a"))

    Transformation.applyTransformations(df, transformations).as[(sql.Date, String)].collect() should contain allElementsOf
      Seq(
        (sql.Date.valueOf(LocalDate.of(1900, 1, 1)), "test"),
        (sql.Date.valueOf(LocalDate.of(2000, 12, 31)), "test")
      )
  }

  "ToUtcTimestamp" should "cast a timestamp into a UTC timestamp" in {

    val df =
      Seq((java.sql.Timestamp.valueOf("1900-01-01 05:23:34.1234"), "test"), (java.sql.Timestamp.valueOf("1900-01-01 12:23:34.1234"), "test"),
          (java.sql.Timestamp.valueOf("1900-01-01 17:23:34.1234"), "test"), (java.sql.Timestamp.valueOf("1900-01-01 23:59:59.1234"), "test"),
          (java.sql.Timestamp.valueOf("1999-12-31 21:30:30.1234"), "test"))
        .toDF("a", "c")

    val transformations = List(ToUtcTimestamps("America/Montreal"))
    val transformations2 = List(ToUtcTimestamps("America/Vancouver", "a"))

    Transformation.applyTransformations(df, transformations).as[(sql.Timestamp, String)].collect() should contain allElementsOf
      Seq(
        (java.sql.Timestamp.valueOf("1900-01-01 10:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-01 17:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-01 22:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-02 04:59:59.1234"), "test"),
        (java.sql.Timestamp.valueOf("2000-01-01 02:30:30.1234"), "test")
      )

    Transformation.applyTransformations(df, transformations2).as[(sql.Timestamp, String)].collect() should contain allElementsOf
      Seq(
        (java.sql.Timestamp.valueOf("1900-01-01 13:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-01 20:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-02 01:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-02 07:59:59.1234"), "test"),
        (java.sql.Timestamp.valueOf("2000-01-01 05:30:30.1234"), "test")
      )
  }

  "FromUtcTimestamp" should "cast a UTC timestamp into a local timestamp" in {

    val df =
      Seq((java.sql.Timestamp.valueOf("1900-01-01 10:23:34.1234"), "test"), (java.sql.Timestamp.valueOf("1900-01-01 17:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-01 22:23:34.1234"), "test"), (java.sql.Timestamp.valueOf("1900-01-02 04:59:59.1234"), "test"),
        (java.sql.Timestamp.valueOf("2000-01-01 02:30:30.1234"), "test"))
        .toDF("a", "c")

    val transformations = List(FromUtcTimestamps("America/Montreal", "a"))

    Transformation.applyTransformations(df, transformations).as[(sql.Timestamp, String)].collect() should contain allElementsOf
      Seq(
        (java.sql.Timestamp.valueOf("1900-01-01 05:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-01 12:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-01 17:23:34.1234"), "test"),
        (java.sql.Timestamp.valueOf("1900-01-01 23:59:59.1234"), "test"),
        (java.sql.Timestamp.valueOf("1999-12-31 21:30:30.1234"), "test")
      )
  }

  "DropDuplicates" should "keep one line per partition if a subset is given" in {

    val df = Seq(
      ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 11))),
      ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 2))),
      ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 5))),
      ("2", sql.Date.valueOf(LocalDate.of(2000, 1, 3))),
      ("2", sql.Date.valueOf(LocalDate.of(2000, 1, 1)))
    ).toDF("id", "updated_on")

    Transformation
      .applyTransformations(df, List(DropDuplicates(Seq("id"), col("updated_on").desc)))
      .as[(String, sql.Date)].collect() should contain allElementsOf
      Seq(
        ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 11))),
        ("2", sql.Date.valueOf(LocalDate.of(2000, 1, 3)))
      )

    val transformations = List(DropDuplicates(Seq("id"), col("updated_on").desc))

    Transformation
      .applyTransformations(df, List(DropDuplicates()))
      .count() shouldBe 5
  }

  "InputFileName" should "extract filename" in {

    val df = spark.read.json(input)

    val transformations = List(InputFileName("file_name", Some(".*/filename/(.*)")))

    Transformation.applyTransformations(df, transformations).select("file_name").as[String].collect() should contain allElementsOf
      Seq(
        ("file1.json"),
        ("file1.json")
      )
  }

  "Integer" should "cast a strings into integers" in {

    val df =
      Seq(("1", "2", "test"), ("3", "4", "test"))
        .toDF("a", "b", "c")

    val transformations = List(ToInteger("a", "b"))

    Transformation.applyTransformations(df, transformations).as[(Int, Int, String)].collect() should contain allElementsOf
      Seq((1, 2, "test"), (3, 4, "test"))

  }

  "KeepFirstWithinPartition" should "keep one line per partition" in {

    val df = Seq(
      ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 11))),
      ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 2))),
      ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 5))),
      ("2", sql.Date.valueOf(LocalDate.of(2000, 1, 3))),
      ("2", sql.Date.valueOf(LocalDate.of(2000, 1, 1)))
    ).toDF("id", "updated_on")

    val transformations = List(KeepFirstWithinPartition(Seq("id"), col("updated_on").desc))

    Transformation.applyTransformations(df, transformations).as[(String, sql.Date)].collect() should contain allElementsOf
      Seq(
        ("1", sql.Date.valueOf(LocalDate.of(2000, 1, 11))),
        ("2", sql.Date.valueOf(LocalDate.of(2000, 1, 3)))
      )
  }


  "PBKDF2" should "hash value" in {

    val testData = Seq(
      ("universinformationnel", "456123789")
    ).toDF("SIN", "NAM")

    val expectedResult = Seq(TestTransformationPBKDF2())

    val job = PBKDF2("coda19", 10000, 512, "SIN", "NAM")
    val result = job.transform(testData)
    result.show(false)

    result.count shouldBe 1
    result.as[TestTransformationPBKDF2].collect should contain allElementsOf expectedResult

  }

  "PBKDF2" should "return null if input column is null" in {

    val testData = Seq(
      null,
      "universinformationnel"
    ).toDF("SIN")

    val job = PBKDF2("coda19", 10000, 512, "SIN")
    val result = job.transform(testData)
    result.show(false)

    result.count shouldBe 2
    result.as[String].collect should contain allElementsOf Seq(null, "72245ae61da8e920c321e0bf57f9a1c9aae59b9806bfc3e7344e794a05dfa27dab0e7c03ba2ba64b2deeb310b996e5f7e984a4e51dab13b59026c339e6fc2a5b")

  }

  "Split" should "split string into an array" in {

    val testData = Seq(
      ("|TEST|TEST|", "TEST|TEST")
    ).toDF("a", "b")

    val result = Split("[|]", "a").transform(testData)
    result.show(false)

    result.count shouldBe 1
    result.select("a").as[Array[String]].collect().head should contain allElementsOf Array("TEST", "TEST")
  }

  "When" should "use when function many times as per user needs" in {

    val testData = Seq("Y", "N", "INVALID").toDF("a")
    testData.show(false)

    val job = transformation.When("a2", List(
      (col("a") === "Y", lit(null).cast(StringType)),
      (col("a") === "N", lit("a is No"))
    ), "a is invalid")

    val result = job.transform(testData)
    result.show(false)

    result.count shouldBe 3
    result
      .select("a", "a2")
      .as[(String, String)].collect() should contain allElementsOf Seq(("Y", null), ("N", "a is No"), ("INVALID", "a is invalid"))

  }

  "CamelToSnake" should "return the columns names from CamelCase in snake_case" in {

    val expectedResult = Seq(TestTransformationCamel2Case())

    val testData = Seq(("test1", "test2", "test3")).toDF("FORMULA", "PanelType", "MAP_TO")
    testData.show(false)

    val job = CamelToSnake("FORMULA", "PanelType", "MAP_TO")
    val result = job.transform(testData)
    result.show(false)

    result.count shouldBe 1
    result.as[TestTransformationCamel2Case].collect should contain allElementsOf expectedResult

  }

  "NormalizeColumnName" should "replace replace illegal characters by underscore or ansii value" in {
    val expectedResult = Seq("A_a", "b_b")
    val expectedResult2 = Seq("A_32a", "B_36b", "B_41b_2", "B_41b", "AVC_FOLLII", "AVC_FOLLI_305")
    val expectedResult3 = Seq("B_36b", "B_41b_2", "B_41b", "AVC_FOLLII", "AVC_FOLLI_305")

    val input = Seq(
      ("test", "test"),
      ("test", "test")
    ).toDF("A a", "b$b")

    val input2 = Seq(
      ("test", "test", "test", "test", "test", "")
    ).toDF("A a", "B$b", "B)b", "B_41b", "AVC_FOLLII", "AVC_FOLLIı")

    NormalizeColumnName().transform(input).columns should contain allElementsOf expectedResult
    NormalizeColumnName("A a", "b$b").transform(input).columns should contain allElementsOf expectedResult
    NormalizeColumnName().transform(input2).columns should contain allElementsOf expectedResult2
    NormalizeColumnName("B$b", "B)b", "B_41b", "AVC_FOLLII", "AVC_FOLLIı").transform(input2).columns should contain allElementsOf expectedResult3

  }

  "Rename" should "rename the name of the column" in {

    val expectedResult = Seq(TestTransformationRename())

    val testData = Seq("test").toDF("LOINC")
    testData.show(false)

    val job = Rename(Map("LOINC" -> "loinc_num"))
    val result = job.transform(testData)
    result.show(false)
    result.printSchema()

    result.count shouldBe 1
    result
      .as[TestTransformationRename].collect should contain allElementsOf expectedResult

  }

}