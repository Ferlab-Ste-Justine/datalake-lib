package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.commons.config.Format.DELTA
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, OverWritePartition, Scd2}
import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, DatasetConf, SimpleConfiguration, StorageConf, TableConf, WriteOptions}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, SparkSpec}
import org.apache.spark.sql.DataFrame

import java.sql.Date
import java.time.LocalDate

class DatasetConfImplicitsSpec extends SparkSpec with CleanUpBeforeEach {

  val tableName = "my_table"
  val databaseName = "default"

  import spark.implicits._

  val overWritePartitionDs: DatasetConf = DatasetConf("ow_ds", "default", "/ow", DELTA, OverWritePartition, partitionby = List("date"))
  val scd2Ds: DatasetConf = DatasetConf("scd2_ds", "default", "/scd2", DELTA, Scd2, writeoptions = WriteOptions.DEFAULT_OPTIONS)
  val placeholderDs: DatasetConf = DatasetConf("placeholder_ds", "default", "/path/placeholder", DELTA, OverWrite, table = Some(TableConf("default", "placeholder_table")))

  lazy implicit val conf: SimpleConfiguration = SimpleConfiguration(DatalakeConf(
    sources = List(overWritePartitionDs, scd2Ds, placeholderDs),
    storages = List(StorageConf("default", this.getClass.getClassLoader.getResource(".").getFile, LOCAL))))

  // We only need to clean placeholderDs since it's the only one with a TableConf (and thus creates a table in the metastore).
  // If we don't drop it from the metastore, we get a DELTA_TABLE_LOCATION_MISMATCH error.
  override val dsToClean: List[DatasetConf] = List(placeholderDs)

  val testData: DataFrame = Seq(
    ("1", true, Date.valueOf(LocalDate.of(1900, 1, 1))),
    ("1", false, Date.valueOf(LocalDate.of(1900, 2, 1))),
    ("2", false, Date.valueOf(LocalDate.of(1900, 2, 1))),
    ("1", false, Date.valueOf(LocalDate.of(1900, 3, 1))),
    ("2", true, Date.valueOf(LocalDate.of(1900, 3, 1)))
  ).toDF("oid", "is_current", "date")

  "tableExist" should "return true if table exists in Spark catalog" in {
    withOutputFolder("root") { root =>
      val updatedConf: Configuration = updateConfStorages(conf, root)

      // We use placeHolderDs since it's the only test ds with TableConf
      LoadResolver
        .write(spark, updatedConf)(placeholderDs.format, placeholderDs.loadtype)
        .apply(placeholderDs, testData)

      placeholderDs.tableExist shouldBe true
      overWritePartitionDs.tableExist shouldBe false
      scd2Ds.tableExist shouldBe false
    }
  }

  "readCurrent" should "return the last partition for a partitioned dataset" in {
    withOutputFolder("root") { root =>
      val updatedConf: Configuration = updateConfStorages(conf, root)

      LoadResolver
        .write(spark, updatedConf)(overWritePartitionDs.format, overWritePartitionDs.loadtype)
        .apply(overWritePartitionDs, testData)

      overWritePartitionDs
        .readCurrent(updatedConf, spark)
        .drop("is_current") // Not relevant for this test
        .as[(String, Date)]
        .collect() should contain theSameElementsAs Seq(
        ("1", Date.valueOf(LocalDate.of(1900, 3, 1))),
        ("2", Date.valueOf(LocalDate.of(1900, 3, 1)))
      )
    }
  }

  it should "return current data for an scd2 table" in {
    withOutputFolder("root") { root =>
      val updatedConf: Configuration = updateConfStorages(conf, root)

      LoadResolver
        .write(spark, updatedConf)(scd2Ds.format, OverWrite) // To avoid SCD2 logic during write
        .apply(scd2Ds, testData)

      scd2Ds
        .readCurrent(updatedConf, spark)
        .as[(String, Boolean, Date)]
        .collect() should contain theSameElementsAs Seq(
        ("1", true, Date.valueOf(LocalDate.of(1900, 1, 1))),
        ("2", true, Date.valueOf(LocalDate.of(1900, 3, 1)))
      )
    }
  }

  "replaceTableName" should "clean invalid characters from table name" in {
    val invalidReplacement = "new.$name.1"
    val validReplacement = "new__name_1"

    val updatedDs = placeholderDs.replaceTableName("placeholder", invalidReplacement, clean = true)

    updatedDs.table.get.name shouldBe s"${validReplacement}_table"
  }

  "replacePlaceholders" should "override a DatasetConf path and tableName" in {
    val customVal = "custom"
    val updatedDs = placeholderDs.replacePlaceholders("placeholder", customVal)

    updatedDs.path shouldBe s"/path/$customVal"
    updatedDs.table.get.name shouldBe s"${customVal}_table"
  }
}
