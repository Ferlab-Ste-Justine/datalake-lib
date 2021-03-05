package bio.ferlab.datalake.core

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class TestInput(a: String = "a", b: Long = 0)

class ClassGeneratorSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  "class generator" should "create a case class for any non-empty dataframe" in {

    val packageName = "ca.ferlab.datalake.core"
    val outputClass = "TestClassOutput"

    val outputStr = ClassGenerator
      .getCaseClassFileContent(packageName, outputClass, Seq(TestInput()).toDF)

    outputStr should contain
      s"""
        |package $packageName
        |
        |
        |case class $outputClass(`a`: String = "a",
        |                           `b`: Long = 0)
        """.stripMargin

  }

  "class generator" should "throw exeception if the input dataframe is empty" in {
    assertThrows[IllegalArgumentException] {
      ClassGenerator.getCaseClassFileContent("ca.test", "test", spark.emptyDataFrame)
    }
  }

}
