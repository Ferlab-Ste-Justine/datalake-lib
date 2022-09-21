package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, DatasetConf, SimpleConfiguration, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import bio.ferlab.datalake.spark3.transformation._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.time.LocalDateTime
import scala.util.Try

class RawToNormalizedETLSpec extends AnyFlatSpec with GivenWhenThen with Matchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  implicit val conf: Configuration = SimpleConfiguration(DatalakeConf(storages = List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile, LOCAL),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile, LOCAL)
  )))

  val srcConf: DatasetConf =  DatasetConf("raw_airports", "raw"       , "/airports.csv", CSV  , OverWrite, Some(TableConf("raw_db" , "raw_airports")), readoptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DatasetConf = DatasetConf("airport"     , "normalized", "/airports"    , DELTA, OverWrite, Some(TableConf("normalized_db", "airport")), keys = List("airport_id"))

  val job: RawFileToNormalizedETL = new RawFileToNormalizedETL(srcConf, destConf,
    List(
      DuplicateColumn("id", "hash_id") -> SHA1("", "hash_id"),
      ToLong("id"),
      Trim("CODE", "description"),
      InputFileName("input_file_name"),
      CurrentTimestamp("createdOn"),
      Rename(Map(
        "id" -> "airport_id",
        "CODE" -> "airport_cd",
        "description" -> "description_EN"
      ))
    )
  )

  override def beforeAll(): Unit = {
    Try(job.reset())
  }

  "RawToNormalizedETL extract" should "return the expected format" in {
    import spark.implicits._

    val data = job.extract()
    data(srcConf.id).as[AirportInput]
  }

  "RawToNormalizedETL transform and publish" should "return the expected format" in {
    import spark.implicits._

    val input = job.extract()
    val output = job.transformSingle(input, LocalDateTime.now(), LocalDateTime.now())
    val head = output.as[AirportOutput].where(col("airport_id") === 1).collect().head
    head shouldBe AirportOutput(input_file_name = head.input_file_name, createdOn = head.createdOn)

    job.publish()
    val files = FileSystemResolver.resolve(conf.getStorage(srcConf.storageid).filesystem)
      .list(srcConf.location.replace("landing", "archive"), true)
    files.foreach(f => println(f.path))
    files.head.name shouldBe "airports.csv"
  }

  "RawToNormalizedETL load" should "create the expected table" in {
    import spark.implicits._

    val output = Seq(AirportOutput()).toDF()

    job.loadSingle(output)

    val table = spark.table(s"${destConf.table.get.fullName}")
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

}
