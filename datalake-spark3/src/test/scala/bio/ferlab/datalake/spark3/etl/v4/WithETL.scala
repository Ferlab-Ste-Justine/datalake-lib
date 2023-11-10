package bio.ferlab.datalake.spark3.etl.v4

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, Upsert}
import bio.ferlab.datalake.commons.config.{DatalakeConf, DatasetConf, SimpleConfiguration, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, CreateDatabasesBeforeAll, SparkSpec}
import org.apache.log4j.{Level, Logger}

trait WithETL extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeEach {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val srcConf: DatasetConf = DatasetConf("raw_airports", "raw", "/airports.csv", CSV, OverWrite, Some(TableConf("raw_db", "raw_airports")), readoptions = Map("header" -> "true", "delimiter" -> "|"))
  val destConf: DatasetConf = DatasetConf("airport", "normalized", "/airports", DELTA, Upsert, Some(TableConf("normalized_db", "airport")), keys = List("airport_id"))

  implicit val conf: SimpleConfiguration = SimpleConfiguration(DatalakeConf(storages = List(
    StorageConf("raw", getClass.getClassLoader.getResource("raw/landing").getFile, LOCAL),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized").getFile, LOCAL)),
    sources = List(srcConf, destConf)
  ))

  type T
  val defaultJob: ETL[T, SimpleConfiguration]
  override val dbToCreate: List[String] = List("raw_db", "normalized_db")
  override val dsToClean: List[DatasetConf] = List(destConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    defaultJob.reset()
  }
}
