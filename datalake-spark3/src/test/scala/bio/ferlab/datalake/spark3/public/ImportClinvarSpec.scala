package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, DatasetConf, StorageConf}
import bio.ferlab.datalake.spark3.testmodels.{ClinvarInput, ClinvarOutput}
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import io.delta.tables.DeltaTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.io.File

class ImportClinvarSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  implicit val conf: Configuration = ConfigurationLoader.loadFromResources("config/reference_kf.conf")
    .copy(storages = List(StorageConf("kf-strides-variant", getClass.getClassLoader.getResource(".").getFile)))

  val clinvar_vcf: DatasetConf = conf.getDataset("clinvar_vcf")
  val clinvar_delta: DatasetConf = conf.getDataset("clinvar")

  override def beforeAll(): Unit = {
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${clinvar_delta.table.map(_.database).getOrElse("variant")}")
      new File(clinvar_delta.location).delete()
    }
  }

  "transform" should "transform ClinvarInput to ClinvarOutput" in {
    val inputData = Map(clinvar_vcf.id -> Seq(ClinvarInput("2"), ClinvarInput("3")).toDF())

    val resultDF = new ImportClinvar().transform(inputData)

    val expectedResults = Seq(ClinvarOutput("2"), ClinvarOutput("3"))

    resultDF.as[ClinvarOutput].collect() should contain allElementsOf(expectedResults)
  }

  "load" should "upsert data" in {
    val firstLoad = Seq(ClinvarOutput("1", name = "first"), ClinvarOutput("2"))
    val secondLoad = Seq(ClinvarOutput("1", name = "second"), ClinvarOutput("3"))
    val expectedResults = Seq(ClinvarOutput("1", name = "second"), ClinvarOutput("2"), ClinvarOutput("3"))

    val job = new ImportClinvar()
    job.load(firstLoad.toDF())
    val firstResult = spark.read.format("delta").load(clinvar_delta.location)
    firstResult.select("chromosome", "start", "end", "reference", "alternate", "name").show(false)
    firstResult.as[ClinvarOutput].collect() should contain allElementsOf firstLoad

    job.load(secondLoad.toDF())
    val secondResult = spark.read.format("delta").load(clinvar_delta.location)
    secondResult.select("chromosome", "start", "end", "reference", "alternate", "name").show(false)
    secondResult.as[ClinvarOutput].collect() should contain allElementsOf expectedResults

    DeltaTable.forName("variant.clinvar").history().show(false)
    spark.sql("DESCRIBE DETAIL variant.clinvar").show(false)
  }

}



