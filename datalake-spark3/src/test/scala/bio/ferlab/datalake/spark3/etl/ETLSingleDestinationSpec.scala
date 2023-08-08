package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType._
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.commons.file.{FileSystemResolver, HadoopFileSystem}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import scala.util.Try

class ETLSingleDestinationSpec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  val srcConf: DatasetConf = DatasetConf("raw_airports", "raw", "/airports.csv", CSV, OverWrite, Some(TableConf("raw_db", "raw_airports")), readoptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DatasetConf = DatasetConf("airport", "normalized", "/airports", DELTA, Upsert, Some(TableConf("normalized_db", "airport")), keys = List("airport_id"))
  implicit val conf: Configuration = SimpleConfiguration(DatalakeConf(storages = List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile, LOCAL),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile, LOCAL)),
    sources = List(srcConf, destConf)
  ))

  override val dbToCreate: List[String] = List("raw_db", "normalized_db")
  override val dsToClean: List[DatasetConf] = List(destConf)

  case class TestETL() extends ETLSingleDestination() {

    var repartitioned = false


    override val mainDestination: DatasetConf = destConf

    override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
      Map(srcConf.id -> spark.read.format(srcConf.format.sparkFormat).options(srcConf.readoptions).load(srcConf.location))
    }

    override def transformSingle(data: Map[String, DataFrame],
                           lastRunDateTime: LocalDateTime = minDateTime,
                           currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
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


    override def defaultRepartition: DataFrame => DataFrame = df=> {
      repartitioned = true
      df
    }

    override def sampling: PartialFunction[String, DataFrame => DataFrame] = {
      case "raw_airports" => {df => df.limit(1)}
    }

    override def reset()(implicit spark: SparkSession): Unit = {
      Try {
        val fs = FileSystemResolver.resolve(conf.getStorage(srcConf.storageid).filesystem)       // get source dataset file system
        val files = fs.list(srcConf.location.replace("landing", "archive"), recursive = true)    // list all archived files
        files.foreach(f => {
          log.info(s"Moving ${f.path} to ${f.path.replace("archive", "landing")}")
          fs.move(f.path, f.path.replace("archive", "landing"), overwrite = true)
        })
      }                                                                                          // move archived files to landing zone
      super.reset()                                                                              // call parent's method to reset destination
    }

  }

  val job: TestETL = TestETL()

  "extract" should "return the expected format" in {
    import spark.implicits._

    val data = job.extract()
    data(srcConf.id).as[AirportInput]
    data(srcConf.id).show(false)
  }

  "transformSingle" should "return the expected format" in {
    import spark.implicits._

    val input = job.extract()
    val output = job.transformSingle(input)
    output.as[AirportOutput]
    output.show(false)
  }

  "loadSingle" should "create the expected table" in {
    import spark.implicits._

    val output = Seq(AirportOutput()).toDF()
    job.repartitioned = false
    job.loadSingle(output)
    assert(job.repartitioned, "Repartition function is not called.")

    val table = spark.table(s"${destConf.table.get.fullName}")
    table.show(false)

    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

  "lastRunDate" should "return minDateTime when destination is empty" in {
    val scd1Conf = destConf.copy(loadtype = Scd1)
    val scd2Conf = destConf.copy(loadtype = Scd2)
    job.getLastRunDateFor(destConf) shouldBe job.minDateTime
    job.getLastRunDateFor(scd1Conf) shouldBe job.minDateTime
    job.getLastRunDateFor(scd2Conf) shouldBe job.minDateTime
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


    job.getLastRunDateFor(scd1Conf) shouldBe date2.toLocalDateTime
  }

  "lastRunDate" should "return max valid_from as LocalDateTime for scd2" in {
    import spark.implicits._
    val scd2Conf = destConf.copy(loadtype = Scd2, path = "/airport_scd2", table = Some(TableConf("normalized_db", "airport_scd2")))
    val date1 = Date.valueOf(LocalDate.of(1900, 1, 2))
    val date2 = Date.valueOf(LocalDate.of(1900, 1, 3))
    val infinity = Date.valueOf(job.maxDateTime.toLocalDate)
    val df = Seq(
      ("1", date1, infinity),
      ("1", date2, infinity),
      ("2", date1, infinity)
    ).toDF("id", "valid_from", "valid_to")

    df.write.format("delta").mode("overwrite")
      .option("mergeSchema", "true")
      .option("path", scd2Conf.location)
      .saveAsTable("normalized_db.airport_scd2")


    job.getLastRunDateFor(scd2Conf) shouldBe date2.toLocalDate.atStartOfDay()
  }

  "skip" should "not run the etl" in {
    HadoopFileSystem.remove(job.mainDestination.location)
    val finalDataframes = job.run(RunStep.getSteps("skip"))
    finalDataframes.isDefinedAt(destConf.id) shouldBe false
  }

  "first_load" should "run the ETL as if it was the first time running" in {
    import spark.implicits._

    job.loadSingle(Seq(AirportOutput(11)).toDF())

    val finalDataframes = job.run(RunStep.initial_load)
    val finalDf = finalDataframes(job.mainDestination.id)
    finalDf.show(false)
    finalDf.count() shouldBe 2
    finalDf.where("airport_id=11").count() shouldBe 0
  }


  "sample_load" should "run the ETL with a sampled data and override the current table" in {
    import spark.implicits._

    job.loadSingle(Seq(AirportOutput(11)).toDF())

    val finalDataframes = job.run(RunStep.allSteps)
    val finalDf = finalDataframes(job.mainDestination.id)
    finalDf.show(false)
    finalDf.count() shouldBe 1
    finalDf.where("airport_id=11").count() shouldBe 0
  }

  "incremental_load" should "run the ETL taking into account the past loads" in {
    import spark.implicits._

    job.reset()
    val firstLoad = job.loadSingle(Seq(AirportOutput(999, "test", "test2", "hash", "file")).toDF())
    firstLoad.show(false)
    job.mainDestination.read.show(false)

    val finalDataframes = job.run(RunStep.default_load)
    val finalDf = finalDataframes(job.mainDestination.id)
    finalDf.show(false)
    finalDf.count() shouldBe 3
    finalDf.where("airport_id=999").count() shouldBe 1
  }

}
