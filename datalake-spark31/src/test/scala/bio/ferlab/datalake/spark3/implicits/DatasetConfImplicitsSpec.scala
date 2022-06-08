package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.{OverWritePartition, Scd2}
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, LoadType, StorageConf, TableConf, WriteOptions}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.nio.file.{Files, Path}
import java.time.LocalDate
import java.sql.Date

class DatasetConfImplicitsSpec extends AnyFlatSpec with WithSparkSession with GivenWhenThen with Matchers with BeforeAndAfterAll {

  val tableName = "my_table"
  val databaseName = "default"

  import spark.implicits._

  val output: String = Files.createTempDirectory("DatasetConfImplicitsSpec").toAbsolutePath.toString

  implicit val conf: Configuration = Configuration(
    sources = List(
      DatasetConf("overwritepartition_table", "storageid", "path", DELTA, OverWritePartition, partitionby = List("date")),
      DatasetConf("scd2_table", "storageid", "path2", DELTA, Scd2, writeoptions = WriteOptions.DEFAULT_OPTIONS)
    ),
    storages = List(
      StorageConf("storageid", output, LOCAL)
    )
  )

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
    val dsConf3 = dsConf.copy(table = Some(TableConf("another_schema",tableName)))
    assert(dsConf.tableExist)
    assert(!dsConf2.tableExist)
    assert(!dsConf3.tableExist)

  }


  "readCurrent" should "return last partition" in {
    val ds =  conf.getDataset("overwritepartition_table")
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
    val ds =  conf.getDataset("scd2_table")
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

}
