package bio.ferlab.datalake.spark3.hive

import bio.ferlab.datalake.spark3.hive.VcfHeaderParser.Header
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VcfHeaderParserSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val vcfPath = getClass.getClassLoader.getResource("vcf/test.vcf").getFile

  val expectedResult = List(
    Header("AF_ESP","AF_ESP","Float","allele frequencies from GO-ESP"),
    Header("AF_EXAC","AF_EXAC","Float","allele frequencies from ExAC"),
    Header("AF_TGP","AF_TGP","Float","allele frequencies from TGP"),
    Header("ALLELEID","ALLELEID","Integer","the ClinVar Allele ID")
  )

  "getVcfHeaders" should "parse a vcf file" in {

    val result = VcfHeaderParser.getVcfHeaders(vcfPath)

    result should contain allElementsOf expectedResult
  }

  "writeDocumentationFileAsJson" should "parse a vcf file and output a json file" in {

    val output = s"${vcfPath}.json"
    VcfHeaderParser.writeDocumentationFileAsJson(vcfPath, output)

    val expectedWrittenData = expectedResult.map(_.toHiveFieldComment)

    spark.read.option("multiline", "true").json(output).as[HiveFieldComment].collect() should contain allElementsOf expectedWrittenData
  }

}
