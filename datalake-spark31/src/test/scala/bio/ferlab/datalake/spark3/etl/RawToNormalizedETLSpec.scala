package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import bio.ferlab.datalake.spark3.transformation._
import org.apache.spark.sql.SparkSession
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
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile, S3),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile, S3)
  ))

  val srcConf: DatasetConf =  DatasetConf("raw_airports", "raw"       , "/airports.csv", CSV  , OverWrite, Some(TableConf("raw_db" , "raw_airports")), readoptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DatasetConf = DatasetConf("airport"     , "normalized", "/airports"    , DELTA, OverWrite, Some(TableConf("normalized_db", "airport")), keys = List("airport_id"))

  val job: RawFileToNormalizedETL = new RawFileToNormalizedETL(srcConf, destConf,
    List(
      Copy("id", "hash_id"),
      SHA1("hash_id"),
      ToLong("id"),
      Trim("CODE", "description"),
      InputFileName("input_file_name"),
      Now("createdOn"),
      Rename(Map(
        "id" -> "airport_id",
        "CODE" -> "airport_cd",
        "description" -> "description_EN"
      ))
    )
  )

  "RawToNormalizedETL extract" should "return the expected format" in {
    import spark.implicits._

    val data = job.extract()
    data(srcConf.id).as[AirportInput]
  }

  "RawToNormalizedETL transform and publish" should "return the expected format" in {
    import spark.implicits._

    val input = job.extract()
    val output = job.transform(input)
    output.as[AirportOutput]

    job.publish()
    val files = FileSystemResolver.resolve(conf.getStorage(srcConf.storageid).filesystem)
      .list(srcConf.location.replace("landing", "archive"), true)
    files.foreach(f => println(f.path))
    files.head.name shouldBe "airports.csv"
  }

  "RawToNormalizedETL load" should "create the expected table" in {
    import spark.implicits._

    val output = Seq(AirportOutput()).toDF()

    job.load(output)

    val table = spark.table(s"${destConf.table.get.fullName}")
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

}
