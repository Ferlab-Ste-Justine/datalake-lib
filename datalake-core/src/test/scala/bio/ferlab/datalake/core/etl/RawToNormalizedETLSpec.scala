package bio.ferlab.datalake.core.etl

import bio.ferlab.datalake.core.config.{Configuration, StorageConf}
import bio.ferlab.datalake.core.etl.Formats.{CSV, DELTA}
import bio.ferlab.datalake.core.transformation.Transformation
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


  implicit val conf: Configuration = Configuration(List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile)
  ))

  val srcConf: DataSource = DataSource("raw", "/airports.csv", "raw_db", "raw_airports", CSV, Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DataSource = DataSource("normalized", "/airports", "normalized_db", "airports", DELTA)

  val job: RawToNormalizedETL = new RawToNormalizedETL(srcConf, destConf, List.empty[Transformation])

  "RawToNormalizedETL extract" should "return the expected format" in {
    import spark.implicits._

    val data = job.extract()
    data(srcConf).as[AirportInput]
  }

  "RawToNormalizedETL transform and publish" should "return the expected format" in {
    import spark.implicits._

    val input = job.extract()
    val output = job.transform(input)
    output.as[AirportInput]

    job.publish()
    val files = job.fs.list(srcConf.location.replace("landing", "archive"), true)
    files.foreach(f => println(f.path))
    files.head.name shouldBe "airports.csv"
  }

  "list files" should "work" in {

  }

  "RawToNormalizedETL load" should "create the expected table" in {
    import spark.implicits._

    val output = Seq(AirportOutput()).toDF()

    job.load(output)

    val table = spark.table(s"${destConf.database}.${destConf.name}")
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

}
