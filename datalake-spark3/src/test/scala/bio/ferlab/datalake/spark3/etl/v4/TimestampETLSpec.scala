package bio.ferlab.datalake.spark3.etl.v4

import bio.ferlab.datalake.commons.config.LoadType._
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.etl.AirportOutput
import bio.ferlab.datalake.spark3.testutils.AirportInput
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, TestTimestampETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

class TimestampETLSpec extends WithETL with CleanUpBeforeEach {

  import spark.implicits._

  case class TimestampTestETL(rc: RuntimeTimestampETLContext) extends SimpleTimestampETL(rc) {

    override val mainDestination: DatasetConf = destConf

    override def extract(lastRunValue: LocalDateTime = minValue,
                         currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
      Map(srcConf.id -> spark.read.format(srcConf.format.sparkFormat).options(srcConf.readoptions).load(srcConf.location))
    }

    override def transform(data: Map[String, DataFrame],
                           lastRunValue: LocalDateTime = minValue,
                           currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
      val df = data(srcConf.id)
        .select(
          col("id").cast(LongType) as "airport_id",
          trim(col("CODE")) as "airport_cd",
          trim(col("description")) as "description_EN",
          sha1(col("id")) as "hash_id",
          input_file_name() as "input_file_name",
          current_timestamp() as "createdOn"
        )
      Map(mainDestination.id -> df)
    }

    override def sampling: PartialFunction[String, DataFrame => DataFrame] = {
      case "raw_airports" => df => df.limit(1)
    }
  }

  type T = LocalDateTime
  override val defaultJob: ETL[LocalDateTime, SimpleConfiguration] = TimestampTestETL(TestTimestampETLContext())

  "extract" should "return the expected format" in {
    val data = defaultJob.extract()
    data(srcConf.id).as[AirportInput]
  }

  "transform" should "return the expected format" in {
    val input = defaultJob.extract()
    val output: Map[String, DataFrame] = defaultJob.transform(input)
    output(destConf.id).as[AirportOutput]
  }

  "load" should "create the expected table" in {
    val output = Seq(AirportOutput()).toDF()

    defaultJob.load(Map(destConf.id -> output))

    val table = spark.table(s"${destConf.table.get.fullName}")
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

  "lastRunValue" should "return minValue when destination is empty" in {
    val scd1Conf = destConf.copy(loadtype = Scd1)
    val scd2Conf = destConf.copy(loadtype = Scd2)
    defaultJob.getLastRunValue(destConf) shouldBe defaultJob.minValue
    defaultJob.getLastRunValue(scd1Conf) shouldBe defaultJob.minValue
    defaultJob.getLastRunValue(scd2Conf) shouldBe defaultJob.minValue
  }

  it should "return max update_on for scd1" in {
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


    defaultJob.getLastRunValue(scd1Conf) shouldBe date2.toLocalDateTime
  }

  it should "return max valid_from as LocalDateTime for scd2" in {
    val scd2Conf = destConf.copy(loadtype = Scd2, path = "/airport_scd2", table = Some(TableConf("normalized_db", "airport_scd2")))
    val date1 = Date.valueOf(LocalDate.of(1900, 1, 2))
    val date2 = Date.valueOf(LocalDate.of(1900, 1, 3))
    val infinity = defaultJob.defaultCurrentValue.toLocalDate
    val df = Seq(
      ("1", date1, infinity),
      ("1", date2, infinity),
      ("2", date1, infinity)
    ).toDF("id", "valid_from", "valid_to")

    df.write.format("delta").mode("overwrite")
      .option("mergeSchema", "true")
      .option("path", scd2Conf.location)
      .saveAsTable("normalized_db.airport_scd2")


    defaultJob.getLastRunValue(scd2Conf) shouldBe date2.toLocalDate.atStartOfDay()
  }

  "skip" should "not run the etl" in {
    val job = TimestampTestETL(TestTimestampETLContext(RunStep.getSteps("skip")))
    HadoopFileSystem.remove(job.mainDestination.location)
    val finalDf = job.run()
    finalDf.isDefinedAt(destConf.id) shouldBe false
  }

  "initial_load" should "run the ETL as if it were the first time running" in {

    val job = TimestampTestETL(TestTimestampETLContext(RunStep.initial_load))
    job.load(Map(destConf.id -> Seq(AirportOutput(11)).toDF()))

    val finalDf = job.run()
    finalDf(destConf.id).count() shouldBe 2
    finalDf(destConf.id).where($"airport_id" === 11).count() shouldBe 0
  }

  "sample_load" should "run the ETL with a sampled data and override the current table" in {

    val job = TimestampTestETL(TestTimestampETLContext(RunStep.allSteps))
    job.load(Map(destConf.id -> Seq(AirportOutput(11)).toDF()))

    val finalDf = job.run()
    finalDf(destConf.id).count() shouldBe 1
    finalDf(destConf.id).where($"airport_id" === 11).count() shouldBe 0
  }
}
