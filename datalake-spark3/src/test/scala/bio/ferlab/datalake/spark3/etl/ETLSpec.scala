package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf, StorageConf, TableConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, DELTA}
import bio.ferlab.datalake.spark3.loader.LoadType._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class ETLSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  implicit val conf: Configuration = Configuration(storages = List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile)),
    sources = List()
  )

  val srcConf: DatasetConf =  DatasetConf("raw_airports", "raw"       , "/airports.csv", CSV  , OverWrite, Some(TableConf("raw_db" , "raw_airports")), readoptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DatasetConf = DatasetConf("airport"     , "normalized", "/airports"    , DELTA, OverWrite, Some(TableConf("normalized_db", "airport")), keys = List("airport_id"))

  case class TestETL() extends ETL() {

    override val destination: DatasetConf = destConf

    override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
      Map(srcConf.id -> spark.read.format(srcConf.format.sparkFormat).options(srcConf.readoptions).load(srcConf.location))
    }

    override def transform(data: Map[String, DataFrame],
                           lastRunDateTime: LocalDateTime = minDateTime,
                           currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
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
  }

  val job: ETL = TestETL()

  "extract" should "return the expected format" in {
    import spark.implicits._

    val data = job.extract()
    data(srcConf.id).as[AirportInput]
    data(srcConf.id).show(false)
  }

  "transform" should "return the expected format" in {
    import spark.implicits._

    val input = job.extract()
    val output = job.transform(input)
    output.as[AirportOutput]
    output.show(false)
  }

  "load" should "create the expected table" in {
    import spark.implicits._

    val output = Seq(AirportOutput()).toDF()

    job.load(output)

    val table = spark.table(s"${destConf.table.get.fullName}")
    table.show(false)
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

}
