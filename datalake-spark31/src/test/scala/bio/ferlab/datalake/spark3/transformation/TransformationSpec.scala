package bio.ferlab.datalake.spark3.transformation

import bio.ferlab.datalake.spark3.model.TestTransformationPBKDF2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql
import java.time.LocalDate

class TransformationSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  val input: String = getClass.getClassLoader.getResource("filename/").getFile + "file1.json"

  import spark.implicits._

  "Cast" should "cast a list of columns into another type" in {

    val df =
      Seq(("1", "2", "test"), ("3", "4", "test"))
        .toDF("a", "b", "c")

    val transformations = List(Cast(IntegerType, "a", "b"))

    Transformation.applyTransformations(df, transformations).as[(Int, Int, String)].collect() should contain allElementsOf
      Seq((1, 2, "test"), (3, 4, "test"))

  }

  "Date" should "cast a strings into a date" in {

    val df =
      Seq(("1900-01-01", "test"), ("2000-12-31", "test"))
        .toDF("a", "c")

    val transformations = List(ToDate("yyyy-MM-dd", "a"))

    Transformation.applyTransformations(df, transformations).as[(sql.Date, String)].collect() should contain allElementsOf
      Seq(
        (sql.Date.valueOf(LocalDate.of(1900, 1, 1)), "test"),
        (sql.Date.valueOf(LocalDate.of(2000, 12, 31)), "test")
      )
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

    val job = new PBKDF2("coda19", 10000, 512, "SIN", "NAM")
    val result = job.transform(testData)
    result.show(false)

    result.count shouldBe 1
    result.as[TestTransformationPBKDF2].collect should contain allElementsOf expectedResult

  }

}
