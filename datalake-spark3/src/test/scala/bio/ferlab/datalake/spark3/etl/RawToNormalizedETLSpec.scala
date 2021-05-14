package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.{CSV, DELTA}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite
import bio.ferlab.datalake.spark3.transformation.Custom
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RawToNormalizedETLSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  implicit val conf: Configuration = Configuration(storages = List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile)
  ))

  val srcConf: SourceConf = SourceConf("raw", "/airports.csv", "raw_db", "raw_airports", CSV, OverWrite, readoptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: SourceConf = SourceConf("normalized", "/airports", "normalized_db", "airport", DELTA, OverWrite, keys = List("airport_id"))

  val job: RawToNormalizedETL = new RawToNormalizedETL(srcConf, destConf, List(Custom(_.select(
    col("id").cast(LongType) as "airport_id",
    trim(col("CODE")) as "airport_cd",
    trim(col("description")) as "description_EN",
    sha1(col("id")) as "hash_id",
    input_file_name() as "input_file_name",
    current_timestamp() as "createdOn"
  ))))

  "RawToNormalizedETL extract" should "return the expected format" in {
    import spark.implicits._

    val data = job.extract()
    data(srcConf).as[AirportInput]
  }

  "RawToNormalizedETL transform and publish" should "return the expected format" in {
    import spark.implicits._

    val input = job.extract()
    val output = job.transform(input)
    output.as[AirportOutput]

    job.publish()
    val files = job.fs.list(srcConf.location.replace("landing", "archive"), true)
    files.foreach(f => println(f.path))
    files.head.name shouldBe "airports.csv"
  }

  "RawToNormalizedETL load" should "create the expected table" in {
    import spark.implicits._

    val output = Seq(AirportOutput()).toDF()

    job.load(output)

    val table = spark.table(s"${destConf.database}.${destConf.name}")
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

}
