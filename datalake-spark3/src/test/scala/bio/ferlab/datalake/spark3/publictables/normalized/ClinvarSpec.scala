package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedClinvar
import bio.ferlab.datalake.spark3.testmodels.raw.RawClinvar
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.WithSparkSession
import io.delta.tables.DeltaTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.io.File
import scala.util.Try

class ClinvarSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_clinvar")
  val destination: DatasetConf = conf.getDataset("normalized_clinvar")

  override def beforeAll(): Unit = {
    Try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${destination.table.map(_.database).getOrElse("variant")}")
      new File(destination.location).delete()
    }
  }

  "transform" should "transform ClinvarInput to ClinvarOutput" in {
    val inputData = Map(source.id -> Seq(RawClinvar("2"), RawClinvar("3")).toDF())

    val resultDF = new Clinvar().transformSingle(inputData)

    val expectedResults = Seq(NormalizedClinvar("2"), NormalizedClinvar("3"))

    resultDF.as[NormalizedClinvar].collect() should contain allElementsOf(expectedResults)
  }

  "load" should "overwrite data" in {
    val firstLoad = Seq(NormalizedClinvar("1", name = "first"), NormalizedClinvar("2"))
    val secondLoad = Seq(NormalizedClinvar("1", name = "second"), NormalizedClinvar("3"))
    val expectedResults = Seq(NormalizedClinvar("1", name = "second"), NormalizedClinvar("3"))

    val job = new Clinvar()
    job.loadSingle(firstLoad.toDF())
    val firstResult = spark.read.format("delta").load(destination.location)
    firstResult.select("chromosome", "start", "end", "reference", "alternate", "name").show(false)
    firstResult.as[NormalizedClinvar].collect() should contain allElementsOf firstLoad

    job.loadSingle(secondLoad.toDF())
    val secondResult = spark.read.format("delta").load(destination.location)
    secondResult.select("chromosome", "start", "end", "reference", "alternate", "name").show(false)
    secondResult.as[NormalizedClinvar].collect() should contain allElementsOf expectedResults

    DeltaTable.forName("variant.clinvar").history().show(false)
    spark.sql("DESCRIBE DETAIL variant.clinvar").show(false)
  }

}



