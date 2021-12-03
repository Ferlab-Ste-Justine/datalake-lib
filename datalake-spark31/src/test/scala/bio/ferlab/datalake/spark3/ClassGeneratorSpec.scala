package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.spark3.ClassGenerator.getClass
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID


case class TestInput(a: String = "a", b: Long = 0, c: String = "c", d: List[String] = List("c", "d"), e: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00"))

class ClassGeneratorSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  "class generator" should "create a case class for empty list" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(d = List())).toDF, true)

    val expectedResult =
      """
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List(),
                           `e`: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00.0"))"""

    outputStr shouldBe expectedResult

  }

  "class generator" should "create a case class for any non-empty dataframe" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput()).toDF, failsOnEmptyDataFrame = true)

    val expectedResult =
"""
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List("c", "d"),
                           `e`: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00.0"))"""

    outputStr shouldBe expectedResult

  }

  "class generator" should "create a case class for any empty dataframe" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput()).toDF.limit(0), failsOnEmptyDataFrame = false)

    val expectedResult =
"""
case class TestClassOutput(`a`: String,
                           `b`: Long,
                           `c`: String,
                           `d`: List[String],
                           `e`: Timestamp)"""

    outputStr shouldBe expectedResult

  }


  "class generator" should "create a case class using row with the least nulls" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(a=null), TestInput(), TestInput(a=null, c=null)).toDF, true)

    val expectedResult =
      """
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List("c", "d"),
                           `e`: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00.0"))"""

    outputStr shouldBe expectedResult

  }

  "class generator" should "create a case class using row with the least nulls (2)" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(a=null), TestInput(a=null, c=null)).toDF, true)

    val expectedResult =
      """
case class TestClassOutput(`a`: Option[String] = None,
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List("c", "d"),
                           `e`: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00.0"))"""

    outputStr shouldBe expectedResult

  }


  "class generator" should "throw exeception if the input dataframe is empty" in {
    assertThrows[IllegalArgumentException] {
      ClassGenerator.getCaseClassFileContent("ca.test", "test", spark.emptyDataFrame)
    }
  }

  "class generator" should "create folder if not exists" in {
    val path = this.getClass.getClassLoader.getResource(".").getFile + UUID.randomUUID().toString
    ClassGenerator.writeCLassFile("ca.test", "test", Seq(TestInput()).toDF(), path)
  }

  "class generator" should "add import for Timestamps" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.getCaseClassFileContent(
      "ca.test",
      outputClass,
      Seq(TestInput(d = List())).toDF,
      LocalDateTime.of(1900, 1, 1, 0, 0, 0))

    val expectedResult =
    """/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 1900-01-01T00:00
 */
package ca.test

import java.sql.Timestamp


case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List(),
                           `e`: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00.0"))

"""

    outputStr shouldBe expectedResult

  }

}
