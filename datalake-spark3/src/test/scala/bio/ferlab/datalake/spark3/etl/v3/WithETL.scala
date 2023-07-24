package bio.ferlab.datalake.spark3.etl.v3

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, Upsert}
import bio.ferlab.datalake.commons.config.{DatalakeConf, DatasetConf, SimpleConfiguration, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.testutils.WithSparkSession
import org.apache.log4j.{Level, Logger}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

trait WithETL extends AnyFlatSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with WithSparkSession {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val srcConf: DatasetConf = DatasetConf("raw_airports", "raw", "/airports.csv", CSV, OverWrite, Some(TableConf("raw_db", "raw_airports")), readoptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DatasetConf = DatasetConf("airport", "normalized", "/airports", DELTA, Upsert, Some(TableConf("normalized_db", "airport")), keys = List("airport_id"))


  implicit val conf: SimpleConfiguration = SimpleConfiguration(DatalakeConf(storages = List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile, LOCAL),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized").getFile, LOCAL)),
    sources = List(srcConf, destConf)
  ))

  val defaultJob: ETL[SimpleConfiguration]

  override def beforeAll(): Unit = {
    spark.sql("CREATE DATABASE IF NOT EXISTS raw_db")
    spark.sql("CREATE DATABASE IF NOT EXISTS normalized_db")
    defaultJob.reset()
  }
}
