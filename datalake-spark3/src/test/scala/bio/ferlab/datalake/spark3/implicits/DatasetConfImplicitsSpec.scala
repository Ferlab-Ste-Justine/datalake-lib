package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, OverWritePartition, Scd2}
import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, DatasetConf, LoadType, SimpleConfiguration, StorageConf, TableConf, WriteOptions}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.SparkSpec

import java.nio.file.Files
import java.sql.Date
import java.time.LocalDate

class DatasetConfImplicitsSpec extends SparkSpec {

  val tableName = "my_table"
  val databaseName = "default"

  import spark.implicits._

  val output: String = Files.createTempDirectory("DatasetConfImplicitsSpec").toAbsolutePath.toString

  implicit val conf: Configuration = SimpleConfiguration(DatalakeConf(
    sources = List(
      DatasetConf("overwritepartition_table", "storageid", "path", DELTA, OverWritePartition, partitionby = List("date")),
      DatasetConf("scd2_table", "storageid", "path2", DELTA, Scd2, writeoptions = WriteOptions.DEFAULT_OPTIONS),
      DatasetConf("placeholder_table", "storageid", "path/placeholder", DELTA, OverWrite, table = TableConf("default", "placeholder_table"))
    ),
    storages = List(
      StorageConf("storageid", output, LOCAL)
    )
  ))

  withOutputFolder("output") { output =>
    val df =
      Seq(("1", "2", "test"), ("3", "4", "test"))
        .toDF("a", "b", "c")
    df.write
      .mode("overwrite")
      .format("parquet")
      .option("path", output)
      .saveAsTable(s"$databaseName.$tableName")
  }

  "tableExist" should "return true if table exist in spark catalog" in {
    val dsConf = DatasetConf("id", "storageid", "path", CSV, LoadType.Read, table = Some(TableConf(databaseName, tableName)))
    val dsConf2 = dsConf.copy(table = Some(TableConf(databaseName, "another_table")))
    val dsConf3 = dsConf.copy(table = Some(TableConf("another_schema", tableName)))
    assert(dsConf.tableExist)
    assert(!dsConf2.tableExist)
    assert(!dsConf3.tableExist)

  }


  "readCurrent" should "return last partition" in {
    val ds = conf.getDataset("overwritepartition_table")
    Seq(
      ("1", Date.valueOf(LocalDate.of(1900, 1, 1))),
      ("1", Date.valueOf(LocalDate.of(1900, 2, 1))),
      ("2", Date.valueOf(LocalDate.of(1900, 2, 1))),
      ("1", Date.valueOf(LocalDate.of(1900, 3, 1))),
      ("2", Date.valueOf(LocalDate.of(1900, 3, 1)))
    ).toDF("id", "date")
      .write
      .mode("overwrite")
      .format("delta").save(ds.location)

    ds.readCurrent.as[(String, Date)].collect() should contain theSameElementsAs Seq(
      ("1", Date.valueOf(LocalDate.of(1900, 3, 1))),
      ("2", Date.valueOf(LocalDate.of(1900, 3, 1)))
    )
  }

  "readCurrent" should "return current data in scd2 table" in {
    val ds = conf.getDataset("scd2_table")
    Seq(
      ("1", true, Date.valueOf(LocalDate.of(1900, 1, 1))),
      ("1", false, Date.valueOf(LocalDate.of(1900, 2, 1))),
      ("2", false, Date.valueOf(LocalDate.of(1900, 2, 1))),
      ("1", false, Date.valueOf(LocalDate.of(1900, 3, 1))),
      ("2", true, Date.valueOf(LocalDate.of(1900, 3, 1)))
    ).toDF("id", "is_current", "date")
      .write
      .mode("overwrite")
      .format("delta").save(ds.location)

    ds.readCurrent.as[(String, Boolean, Date)].collect() should contain theSameElementsAs Seq(
      ("1", true, Date.valueOf(LocalDate.of(1900, 1, 1))),
      ("2", true, Date.valueOf(LocalDate.of(1900, 3, 1)))
    )
  }

  "replaceTableName" should "clean invalid characters from table name" in {
    val invalidReplacement = "new.$name.1"
    val validReplacement = "new__name_1"

    val datasetConf = conf.getDataset("placeholder_table")
      .replaceTableName("placeholder", invalidReplacement, clean = true)

    datasetConf.table.get.name shouldBe s"${validReplacement}_table"
  }

  "replacePlaceholders" should "override a DatasetConf path and tableName" in {
    val ds = conf.getDataset("placeholder_table")
    val customVal = "custom"
    val datasetConf = ds.replacePlaceholders("placeholder", customVal)

    datasetConf.path shouldBe s"path/$customVal"
    datasetConf.table.get.name shouldBe s"${customVal}_table"
  }

}
