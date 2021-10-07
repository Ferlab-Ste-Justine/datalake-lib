package bio.ferlab.datalake.spark3

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


case class TestInput(a: String = "a", b: Long = 0, c: String = "c", d: List[String] = List("c", "d"))

class ClassGeneratorSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  "class generator" should "create a case class for any non-empty dataframe" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput()).toDF)

    val expectedResult =
"""
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List("c", "d"))"""

    outputStr shouldBe expectedResult

  }


  "class generator" should "create a case class using row with the least nulls" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(a=null), TestInput(), TestInput(a=null, c=null)).toDF)

    val expectedResult =
      """
case class TestClassOutput(`a`: String = "a",
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List("c", "d"))"""

    outputStr shouldBe expectedResult

  }

  "class generator" should "create a case class using row with the least nulls (2)" in {

    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator.oneClassString(outputClass, Seq(TestInput(a=null), TestInput(a=null, c=null)).toDF)

    val expectedResult =
      """
case class TestClassOutput(`a`: Option[String] = None,
                           `b`: Long = 0,
                           `c`: String = "c",
                           `d`: List[String] = List("c", "d"))"""

    outputStr shouldBe expectedResult

  }


  "class generator" should "throw exeception if the input dataframe is empty" in {
    assertThrows[IllegalArgumentException] {
      ClassGenerator.getCaseClassFileContent("ca.test", "test", spark.emptyDataFrame)
    }
  }

}
