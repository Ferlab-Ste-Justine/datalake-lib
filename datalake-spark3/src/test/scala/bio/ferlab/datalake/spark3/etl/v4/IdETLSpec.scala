package bio.ferlab.datalake.spark3.etl.v4

import bio.ferlab.datalake.commons.config.LoadType._
import bio.ferlab.datalake.commons.config.WriteOptions.UPDATED_ON_COLUMN_NAME
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.HadoopFileSystem
import bio.ferlab.datalake.spark3.etl.AirportOutput
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.testutils.AirportInput
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, TestIdETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.sql.Date
import java.time.LocalDate

class IdETLSpec extends WithETL with CleanUpBeforeEach {

  import spark.implicits._

  case class IdTestETL(rc: RuntimeIdETLContext) extends SimpleIdETL(rc) {

    override val mainDestination: DatasetConf = destConf

    override def extract(lastRunValue: String = minValue,
                         currentRunValue: String): Map[String, DataFrame] = {
      Map(srcConf.id -> spark.read.format(srcConf.format.sparkFormat).options(srcConf.readoptions).load(srcConf.location))
    }

    override def transform(data: Map[String, DataFrame],
                           lastRunValue: String = minValue,
                           currentRunValue: String): Map[String, DataFrame] = {
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

  type T = String
  override val defaultJob: ETL[String, SimpleConfiguration] = IdTestETL(TestIdETLContext())

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
    defaultJob.getLastRunValue(destConf) shouldBe defaultJob.minValue
  }

  it should "return max id for incremental data" in {
    val updated_on_column_name = "id"
    val date1 = Date.valueOf(LocalDate.of(1900, 1, 2))
    val date2 = Date.valueOf(LocalDate.of(1900, 1, 3))
    val overWritePartitionConf = destConf.copy(
      loadtype = OverWritePartition,
      path = "/airport_overwrite",
      table = Some(TableConf("normalized_db", "airport_overwrite")),
      partitionby = List("date"),
      writeoptions = Map(UPDATED_ON_COLUMN_NAME -> updated_on_column_name))

    // Existing data
    val df = Seq(
      (1, "value1", date1),
      (12345, "value2", date2),
    ).toDF(updated_on_column_name, "value", "date")

    LoadResolver
      .write(spark, conf)
      .apply(overWritePartitionConf.format, overWritePartitionConf.loadtype)
      .apply(overWritePartitionConf, df)

    defaultJob.getLastRunValue(overWritePartitionConf) shouldBe "12345"
  }

  "skip" should "not run the etl" in {
    val job = IdTestETL(TestIdETLContext(RunStep.getSteps("skip")))
    HadoopFileSystem.remove(job.mainDestination.location)
    val finalDf = job.run()
    finalDf.isDefinedAt(destConf.id) shouldBe false
  }

  "initial_load" should "run the ETL as if it were the first time running" in {

    val job = IdTestETL(TestIdETLContext(RunStep.initial_load))
    job.load(Map(destConf.id -> Seq(AirportOutput(11)).toDF()))

    val finalDf = job.run()
    finalDf(destConf.id).count() shouldBe 2
    finalDf(destConf.id).where($"airport_id" === 11).count() shouldBe 0
  }

  "sample_load" should "run the ETL with a sampled data and override the current table" in {

    val job = IdTestETL(TestIdETLContext(RunStep.allSteps))
    job.load(Map(destConf.id -> Seq(AirportOutput(11)).toDF()))

    val finalDf = job.run()
    finalDf(destConf.id).count() shouldBe 1
    finalDf(destConf.id).where($"airport_id" === 11).count() shouldBe 0
  }
}
