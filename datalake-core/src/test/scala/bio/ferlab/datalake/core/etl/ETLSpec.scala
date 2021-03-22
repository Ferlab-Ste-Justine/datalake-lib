package bio.ferlab.datalake.core.etl

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import bio.ferlab.datalake.core.loader.Formats.{CSV, DELTA}
import bio.ferlab.datalake.core.loader.LoadTypes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ETLSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  implicit val conf: Configuration = Configuration(List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile)
  ))

  val srcConf: DataSource = DataSource("raw", "/airports.csv", "raw_db", "raw_airports", CSV, OverWrite, readOptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DataSource = DataSource("normalized", "/airports", "normalized_db", "airport", DELTA, OverWrite, primaryKeys = Seq("airport_id"))

  case class TestETL() extends ETL(destConf) {
    override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
      Map(srcConf -> spark.read.format(srcConf.format.sparkFormat).options(srcConf.readOptions).load(srcConf.location))
    }

    override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
      data(srcConf)
        .select(
          col("id").cast(LongType) as "airport_id",
          trim(col("CODE")) as "airport_cd",
          trim(col("description")) as "description_EN",
          sha1(col("id")) as "hash_id",
          input_file_name() as "input_file_name",
          current_timestamp() as "createdOn"
        )

    }

    override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${destination.database}")
      data
        .write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option("path", destination.location)
        .saveAsTable(s"${destination.database}.${destination.name}")
      data
    }
  }

  val job: ETL = TestETL()

  "extract" should "return the expected format" in {
    import spark.implicits._

    val data = job.extract()
    data(srcConf).as[AirportInput]
    data(srcConf).show(false)
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

    val table = spark.table(s"${destConf.database}.${destConf.name}")
    table.show(false)
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

}
