package bio.ferlab.datalake.spark3.public.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.testmodels.{AnnovarScoresInput, AnnovarScoresOutput}
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.io.File
import scala.util.Try

class AnnovarScoresSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_dbnsfp_annovar")
  val destination: DatasetConf = conf.getDataset("normalized_dbnsfp_annovar")
  val job = new AnnovarScores()

  override def beforeAll(): Unit = {
    Try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${destination.table.map(_.database).getOrElse("variant")}")
      new File(destination.location).delete()
    }
  }

  "transform" should "transform AnnovarScoresInput to AnnovarScoresOutput" in {
    val inputData = Map(source.id -> Seq(AnnovarScoresInput("2"), AnnovarScoresInput("3")).toDF())

    val resultDF = job.transform(inputData)

    //ClassGenerator
    //  .writeCLassFile(
    //    "bio.ferlab.datalake.spark3.testmodels",
    //    "AnnovarScoresOutput",
    //    resultDF,
    //    "datalake-spark3/src/test/scala/")

    val expectedResults = Seq(AnnovarScoresOutput("2"), AnnovarScoresOutput("3"))

    resultDF.as[AnnovarScoresOutput].collect() should contain allElementsOf(expectedResults)
  }

  "load" should "upsert data" in {
    val firstLoad = Seq(AnnovarScoresOutput("1", SIFT_pred = "first"), AnnovarScoresOutput("2"))
    val secondLoad = Seq(AnnovarScoresOutput("1", SIFT_pred = "second"), AnnovarScoresOutput("3"))
    val expectedResults = Seq(AnnovarScoresOutput("1", SIFT_pred = "second"), AnnovarScoresOutput("2"), AnnovarScoresOutput("3"))

    job.load(firstLoad.toDF())
    val firstResult = destination.read
    firstResult.as[AnnovarScoresOutput].collect() should contain allElementsOf firstLoad

    job.load(secondLoad.toDF())
    val secondResult = destination.read
    secondResult.as[AnnovarScoresOutput].collect() should contain allElementsOf expectedResults
    secondResult.show(false)
    
  }

}



