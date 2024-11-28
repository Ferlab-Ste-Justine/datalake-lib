package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.Format.DELTA
import bio.ferlab.datalake.commons.config.LoadType.OverWritePartition
import bio.ferlab.datalake.commons.config.{DatalakeConf, DatasetConf, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll

import java.nio.file.{Files, Path}
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

case class TEST(id: Int, value: Option[String], cond: Boolean, date: Date)

class DeltaUtilsSpec extends SparkSpec with BeforeAndAfterAll {

  import spark.implicits._

  val testDs: DatasetConf = DatasetConf("test_ds", "default", "/test", DELTA, OverWritePartition, partitionby = List("cond", "date"))

  // Use the same temporary folder for all unit tests
  lazy val tempPath: Path = Files.createTempDirectory("temp")
  lazy implicit val conf: SimpleConfiguration = SimpleConfiguration(datalake = DatalakeConf(
    storages = List(StorageConf("default", tempPath.toAbsolutePath.toString, LOCAL)),
    sources = List(testDs)))

  val testDf: DataFrame = Seq(
    TEST(id = 1, value = Some("a"), cond = true, date = Date.valueOf("2020-01-01")),
    TEST(id = 2, value = Some("b"), cond = false, date = Date.valueOf("2020-01-01")),
    TEST(id = 3, value = None, cond = true, date = Date.valueOf("2020-01-02")),
  ).toDF

  override def beforeAll(): Unit = {
    LoadResolver
      .write(spark, conf)(testDs.format, testDs.loadtype)
      .apply(testDs, testDf)
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(tempPath.toFile)
  }

  "getRetentionHours" should "return the difference between now and the oldest timestamp - 1 hour" in {

    val clock = LocalDateTime.of(2020, 10, 9, 2, 30, 0)

    val oldestTimestamp = Timestamp.valueOf("2020-10-07 08:45:00")
    val timestamps = Seq(
      Timestamp.valueOf("2020-10-08 13:56:00"),
      oldestTimestamp,
      Timestamp.valueOf("2020-10-08 15:13:00")
    )
    val l = DeltaUtils.getRetentionHours(timestamps, clock)
    clock.minusHours(l).isBefore(oldestTimestamp.toLocalDateTime) shouldBe true
    l shouldBe 42

  }

  "getPartitionValues" should "return a DataFrame with distinct partition values in a Delta table" in {
    val result: DataFrame = DeltaUtils.getPartitionValues(testDs.location)

    result
      .as[(String, List[String])]
      .collect() should contain theSameElementsAs Seq(
      ("cond", List("false", "true")),
      ("date", List("2020-01-01", "2020-01-02")),
    )
  }

  "getNumRecords" should "return the total number of records in a Delta table" in {
    val result: Long = DeltaUtils.getNumRecords(testDs.location)

    result shouldBe 3
  }

  "getNumRecordsPerPartition" should "return the total number of records for each partition in a Delta table" in {
    val result: DataFrame = DeltaUtils.getNumRecordsPerPartition(testDs.location)

    result
      .as[(String, String, Long)]
      .collect() should contain theSameElementsAs Seq(
      ("cond", "false", 1),
      ("cond", "true", 2),
      ("date", "2020-01-01", 2),
      ("date", "2020-01-02", 1)
    )
  }

  "getMinValues" should "return the minimum value for each non-partition column in a Delta table" in {
    val result: Map[String, Any] = DeltaUtils.getMinValues(testDs.location)

    result should contain theSameElementsAs Map(
      "id" -> 1,
      "value" -> "a",
    )
  }

  "getMinValuesPerPartition" should "return the minimum value for each column for each partition value in a Delta table" in {
    val result: DataFrame = DeltaUtils.getMinValuesPerPartition(testDs.location)

    result
      .select("partitionColumn", "partitionValue", "id")
      .as[(String, String, Int)]
      .collect() should contain theSameElementsAs Seq(
      ("cond", "false", 2),
      ("cond", "true", 1),
      ("date", "2020-01-01", 1),
      ("date", "2020-01-02", 3)
    )

    result
      .select("partitionColumn", "partitionValue", "value")
      .as[(String, String, Option[String])]
      .collect() should contain theSameElementsAs Seq(
      ("cond", "false", Some("b")),
      ("cond", "true", Some("a")),
      ("date", "2020-01-01", Some("a")),
      ("date", "2020-01-02", None)
    )
  }

  "getMaxValues" should "return the maximum value for each non-partition column in a Delta table" in {
    val result: Map[String, Any] = DeltaUtils.getMaxValues(testDs.location)

    result should contain theSameElementsAs Map(
      "id" -> 3,
      "value" -> "b",
    )
  }

  "getMaxValuesPerPartition" should "return the maximum value for each column for each partition value in a Delta table" in {
    val result: DataFrame = DeltaUtils.getMaxValuesPerPartition(testDs.location)

    result
      .select("partitionColumn", "partitionValue", "id")
      .as[(String, String, Int)]
      .collect() should contain theSameElementsAs Seq(
      ("cond", "false", 2),
      ("cond", "true", 3),
      ("date", "2020-01-01", 2),
      ("date", "2020-01-02", 3)
    )

    result
      .select("partitionColumn", "partitionValue", "value")
      .as[(String, String, Option[String])]
      .collect() should contain theSameElementsAs Seq(
      ("cond", "false", Some("b")),
      ("cond", "true", Some("a")),
      ("date", "2020-01-01", Some("b")),
      ("date", "2020-01-02", None)
    )
  }

  "getNullCounts" should "return the total number of nulls for each non-partition column in a Delta table" in {
    val result: Map[String, Long] = DeltaUtils.getNullCounts(testDs.location)

    result should contain theSameElementsAs Map(
      "id" -> 0,
      "value" -> 1
    )
  }

  "getNullCountsPerPartition" should "return the total number of nulls for each column for each partition value in a Delta table" in {
    val result: DataFrame = DeltaUtils.getNullCountsPerPartition(testDs.location)

    result
      .select("partitionColumn", "partitionValue", "id")
      .as[(String, String, Long)]
      .collect() should contain theSameElementsAs Seq(
      ("cond", "false", 0),
      ("cond", "true", 0),
      ("date", "2020-01-01", 0),
      ("date", "2020-01-02", 0)
    )

    result
      .select("partitionColumn", "partitionValue", "value")
      .as[(String, String, Long)]
      .collect() should contain theSameElementsAs Seq(
      ("cond", "false", 0),
      ("cond", "true", 1),
      ("date", "2020-01-01", 0),
      ("date", "2020-01-02", 1)
    )
  }
}
