package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.testutils.SparkSpec

class AnnovarScoresSpec extends SparkSpec {
//TODO need to regenerate input dataframe
  /*

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

    val resultDF = job.transformSingle(inputData)

    //ClassGenerator
    //  .writeCLassFile(
    //    "bio.ferlab.datalake.testutils.models",
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

    job.loadSingle(firstLoad.toDF())
    val firstResult = destination.read
    firstResult.as[AnnovarScoresOutput].collect() should contain allElementsOf firstLoad

    job.loadSingle(secondLoad.toDF())
    val secondResult = destination.read
    secondResult.as[AnnovarScoresOutput].collect() should contain allElementsOf expectedResults
    secondResult.show(false)
    
  }
*/
}



