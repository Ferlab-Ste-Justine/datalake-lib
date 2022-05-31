package bio.ferlab.datalake.spark3.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID


case class TestInput(a: String = "a",
                     b: Long = 0,
                     c: String =
"""{"c":1,
"d":2}""",
                     d: Seq[String] = Seq("c", "d"),
                     e: Timestamp = Timestamp.valueOf("1900-01-01 00:00:00"),
                     f: Array[Byte] = Array(0.toByte, "2".toByte))

class ClassGeneratorSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  "class generator" should "create a case class for empty list" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(d = Seq())).toDF)

    val expectedResult =
      """
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "{\"c\":1, \"d\":2}",
                           `d`: Seq[String] = Seq(),
                           `e`: Timestamp = java.sql.Timestamp.valueOf("1900-01-01 00:00:00.0"),
                           `f`: Array[Byte] = Array(0.toByte, 2.toByte))"""

    outputStr shouldBe expectedResult

  }

  "class generator" should "create a case class for any non-empty dataframe" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput()).toDF)

    val expectedResult =
"""
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "{\"c\":1, \"d\":2}",
                           `d`: Seq[String] = Seq("c", "d"),
                           `e`: Timestamp = java.sql.Timestamp.valueOf("1900-01-01 00:00:00.0"),
                           `f`: Array[Byte] = Array(0.toByte, 2.toByte))"""

    outputStr shouldBe expectedResult

  }

  "class generator" should "create a case class for any empty dataframe" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput()).toDF.limit(0))

    val expectedResult =
"""
case class TestClassOutput(`a`: Option[String] = None,
                           `b`: Option[Long] = None,
                           `c`: Option[String] = None,
                           `d`: Option[Seq[String]] = None,
                           `e`: Option[Timestamp] = None,
                           `f`: Option[Array[Byte]] = None)"""

    outputStr shouldBe expectedResult

  }


  "class generator" should "create a case class using row with the least nulls" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(a=null), TestInput(), TestInput(a=null, c=null)).toDF)

    val expectedResult =
      """
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "{\"c\":1, \"d\":2}",
                           `d`: Seq[String] = Seq("c", "d"),
                           `e`: Timestamp = java.sql.Timestamp.valueOf("1900-01-01 00:00:00.0"),
                           `f`: Array[Byte] = Array(0.toByte, 2.toByte))"""

    outputStr shouldBe expectedResult

  }

  "class generator" should "create a case class using row with the least nulls (2)" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(a=null), TestInput(a=null, c=null)).toDF)

    val expectedResult =
      """
case class TestClassOutput(`a`: Option[String] = None,
                           `b`: Long = 0,
                           `c`: String = "{\"c\":1, \"d\":2}",
                           `d`: Seq[String] = Seq("c", "d"),
                           `e`: Timestamp = java.sql.Timestamp.valueOf("1900-01-01 00:00:00.0"),
                           `f`: Array[Byte] = Array(0.toByte, 2.toByte))"""

    outputStr shouldBe expectedResult

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
      Seq(TestInput(d = Seq())).toDF,
      LocalDateTime.of(1900, 1, 1, 0, 0, 0))

    ClassGenerator.writeCLassFile(
      "bio.ferlab.datalake.spark3.utils",
      outputClass,
      Seq(TestInput(d = Seq())).toDF,
      "datalake-spark31/src/test/scala/")

    val expectedResult =
    """/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 1900-01-01T00:00
 */
package ca.test

import java.sql.Timestamp


case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "{\"c\":1, \"d\":2}",
                           `d`: Seq[String] = Seq(),
                           `e`: Timestamp = java.sql.Timestamp.valueOf("1900-01-01 00:00:00.0"),
                           `f`: Array[Byte] = Array(0.toByte, 2.toByte))

"""

    outputStr shouldBe expectedResult

  }

}
