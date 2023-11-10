package bio.ferlab.datalake.spark3.etl.v3

import bio.ferlab.datalake.commons.config.LoadType._
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.config.DeprecatedRuntimeETLContext
import bio.ferlab.datalake.commons.file.{FileSystemResolver, HadoopFileSystem}
import bio.ferlab.datalake.spark3.etl.{AirportInput, AirportOutput}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.DeprecatedTestETLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import scala.util.Try

class SingleETLSpec extends WithETL {

  import spark.implicits._

  case class TestETL(rc: DeprecatedRuntimeETLContext) extends SimpleSingleETL(rc) {

    var repartitioned = false


    override val mainDestination: DatasetConf = destConf

    override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
      Map(srcConf.id -> spark.read.format(srcConf.format.sparkFormat).options(srcConf.readoptions).load(srcConf.location))
    }

    override def transformSingle(data: Map[String, DataFrame],
                                 lastRunDateTime: LocalDateTime = minDateTime,
                                 currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
      log.info(srcConf.id)
      data(srcConf.id)
        .select(
          col("id").cast(LongType) as "airport_id",
          trim(col("CODE")) as "airport_cd",
          trim(col("description")) as "description_EN",
          sha1(col("id")) as "hash_id",
          input_file_name() as "input_file_name",
          current_timestamp() as "createdOn"
        )

    }


    override def defaultRepartition: DataFrame => DataFrame = df => {
      repartitioned = true
      df
    }

    override def sampling: PartialFunction[String, DataFrame => DataFrame] = {
      case "raw_airports" => df => df.limit(1)
    }

    override def reset(): Unit = {
      Try {
        val fs = FileSystemResolver.resolve(conf.getStorage(srcConf.storageid).filesystem) // get source dataset file system
        val files = fs.list(srcConf.location.replace("landing", "archive"), recursive = true) // list all archived files
        files.foreach(f => {
          log.info(s"Moving ${f.path} to ${f.path.replace("archive", "landing")}")
          fs.move(f.path, f.path.replace("archive", "landing"), overwrite = true)
        })
      } // move archived files to landing zone
      super.reset() // call parent's method to reset destination
    }

  }

  override val defaultJob: TestETL = TestETL(DeprecatedTestETLContext())


  "extract" should "return the expected format" in {

    val data = defaultJob.extract()
    data(srcConf.id).as[AirportInput]
    data(srcConf.id).show(false)
  }

  "transformSingle" should "return the expected format" in {

    val input = defaultJob.extract()
    val output = defaultJob.transformSingle(input)
    output.as[AirportOutput]
    output.show(false)
  }

  "loadSingle" should "create the expected table" in {

    val output = Seq(AirportOutput()).toDF()
    defaultJob.repartitioned = false
    defaultJob.loadSingle(output)
    assert(defaultJob.repartitioned, "Repartition function is not called.")

    val table = spark.table(s"${destConf.table.get.fullName}")
    table.show(false)

    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

  "lastRunDate" should "return minDateTime when destination is empty" in {
    val scd1Conf = destConf.copy(loadtype = Scd1)
    val scd2Conf = destConf.copy(loadtype = Scd2)
    defaultJob.getLastRunDateFor(destConf) shouldBe defaultJob.minDateTime
    defaultJob.getLastRunDateFor(scd1Conf) shouldBe defaultJob.minDateTime
    defaultJob.getLastRunDateFor(scd2Conf) shouldBe defaultJob.minDateTime
  }

  "lastRunDate" should "return max update_on for scd1" in {
    import spark.implicits._
    val scd1Conf = destConf.copy(loadtype = Scd1, path = "/airport_scd1", table = Some(TableConf("normalized_db", "airport_scd1")))
    val date1 = Timestamp.valueOf(LocalDateTime.of(1900, 1, 2, 1, 1, 1))
    val date2 = Timestamp.valueOf(LocalDateTime.of(1900, 1, 3, 1, 1, 1))
    val df = Seq(
      ("1", date1, date1),
      ("2", date1, date2),
      ("3", date1, date1)
    ).toDF("id", "created_on", "updated_on")

    df.write.format("delta").mode("overwrite")
      .option("mergeSchema", "true")
      .option("path", scd1Conf.location)
      .saveAsTable("normalized_db.airport_scd1")


    defaultJob.getLastRunDateFor(scd1Conf) shouldBe date2.toLocalDateTime
  }

  "lastRunDate" should "return max valid_from as LocalDateTime for scd2" in {
    import spark.implicits._
    val scd2Conf = destConf.copy(loadtype = Scd2, path = "/airport_scd2", table = Some(TableConf("normalized_db", "airport_scd2")))
    val date1 = Date.valueOf(LocalDate.of(1900, 1, 2))
    val date2 = Date.valueOf(LocalDate.of(1900, 1, 3))
    val infinity = Date.valueOf(defaultJob.maxDateTime.toLocalDate)
    val df = Seq(
      ("1", date1, infinity),
      ("1", date2, infinity),
      ("2", date1, infinity)
    ).toDF("id", "valid_from", "valid_to")

    df.write.format("delta").mode("overwrite")
      .option("mergeSchema", "true")
      .option("path", scd2Conf.location)
      .saveAsTable("normalized_db.airport_scd2")


    defaultJob.getLastRunDateFor(scd2Conf) shouldBe date2.toLocalDate.atStartOfDay()
  }

  "skip" should "not run the etl" in {
    HadoopFileSystem.remove(defaultJob.mainDestination.location)
    val finalDataframes = defaultJob.run()
    finalDataframes.isDefinedAt(destConf.id) shouldBe false
  }

  "first_load" should "run the ETL as if it was the first time running" in {
    val job = TestETL(DeprecatedTestETLContext(RunStep.initial_load))

    val finalDataframes = job.run()
    val finalDf = finalDataframes(job.mainDestination.id)
    finalDf.show(false)
    finalDf.count() shouldBe 2
    finalDf.where("airport_id=11").count() shouldBe 0
  }


  "sample_load" should "run the ETL with a sampled data and override the current table" in {
    val job = TestETL(DeprecatedTestETLContext(RunStep.allSteps))
    job.loadSingle(Seq(AirportOutput(11)).toDF())

    val finalDataframes = job.run()
    val finalDf = finalDataframes(job.mainDestination.id)
    finalDf.show(false)
    finalDf.count() shouldBe 1
    finalDf.where("airport_id=11").count() shouldBe 0
  }

  "incremental_load" should "run the ETL taking into account the past loads" in {
    val job = TestETL(DeprecatedTestETLContext(RunStep.default_load))
    job.reset()
    val firstLoad = job.loadSingle(Seq(AirportOutput(999, "test", "test2", "hash", "file")).toDF())
    firstLoad.show(false)
    job.mainDestination.read.show(false)

    val finalDataframes = job.run()
    val finalDf = finalDataframes(job.mainDestination.id)
    finalDf.show(false)
    finalDf.count() shouldBe 3
    finalDf.where("airport_id=999").count() shouldBe 1
  }

}
