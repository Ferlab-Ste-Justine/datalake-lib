package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.DELTA
import bio.ferlab.datalake.commons.config.LoadType.Scd2
import bio.ferlab.datalake.commons.config.{DatasetConf, WriteOptions}
import bio.ferlab.datalake.spark3.file.HadoopFileSystem
import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}

case class Scd2Table(id: String,
                     buid: String,
                     value: String,
                     oid: String,
                     ingested_on: Date,
                     valid_from: Date,
                     valid_to: Date,
                     is_current: Boolean)

class DeltaLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val testtableoverwite: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtableoverwite"
  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtable"
  val scd1Output: String = getClass.getClassLoader.getResource("normalized/").getFile + "scd1table"
  val scd2Output: String = getClass.getClassLoader.getResource("normalized/").getFile + "scd2table"

  override def beforeAll(): Unit = {
    spark.sql("DROP TABLE IF EXISTS default.testtable")
    HadoopFileSystem.remove(output)
    spark.sql("DROP TABLE IF EXISTS default.scd1table")
    HadoopFileSystem.remove(scd1Output)
    spark.sql("DROP TABLE IF EXISTS default.scd2table")
    HadoopFileSystem.remove(scd2Output)
  }


  "scd1" should "update existing data and insert new data" in {
    import spark.implicits._

    val tableName = "scd1Table"

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      TestData("a"  , "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaLoader
      .scd1(
        output,
        "default",
        tableName,
        existing,
        Seq("uid"),
        "oid",
        "createdOn",
        "updatedOn",
        List(),
        "delta",
        Map()
      )

    val updates: DataFrame = Seq(
      TestData("a"  , "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aa" , "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    ).toDF

    val expectedResult: Seq[TestData] = Seq(
      TestData("a"  , "b", Timestamp.valueOf(day1), Timestamp.valueOf(day2), 2), //will be updated
      TestData("aa" , "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),//will be inserted
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)//will stay the same
    )

    DeltaLoader.scd1(
      output,
      "default",
      tableName,
      updates,
      Seq("uid"),
      "oid",
      "createdOn",
      "updatedOn",
      List(),
      "delta",
      Map()
    )


    val df = DeltaLoader.read(output, DELTA.sparkFormat, Map(), Some("default"), Some(tableName))
    df.count() shouldBe 3
    df.as[TestData].collect() should contain allElementsOf expectedResult

  }

  "scd2" should "add new row for existing object and insert new objects" in {
    import spark.implicits._

    val ds = DatasetConf("scd2table", "storageid", "/scd2table", DELTA, Scd2)
    val ingestedOnNane = "ingested_on"
    val validFromName = WriteOptions.DEFAULT_VALID_FROM
    val validToName = WriteOptions.DEFAULT_VALID_TO
    val buidName = "buid"
    val oidName = "oid"
    val isCurrentName = "is_current"

    val day1 = LocalDate.of(2020, 1, 1)
    val dayminus1 = day1.minusDays(1)
    val day2 = day1.plusDays(1)
    val day3 = day2.plusDays(1)
    val maxDate = LocalDate.of(9999, 12, 31)

    val existing: DataFrame = Seq(
      ("1", "a", "aa", Date.valueOf(dayminus1), Date.valueOf(dayminus1)), // must be dropped
      ("1", "a", "aa", Date.valueOf(day1), Date.valueOf(day1)),
      ("2", "b", "bb", Date.valueOf(day1), Date.valueOf(day1)),
      ("3", "c", "cc", Date.valueOf(day2), Date.valueOf(day2))
    ).toDF("id", "value", ds.oid, ingestedOnNane, validFromName)

    DeltaLoader
      .scd2(scd2Output, "default", "scd2table", existing, Seq("id"),
        buidName, oidName, isCurrentName, List(), DELTA.sparkFormat, validFromName, validToName, Map(), maxDate)

    val updates: DataFrame = Seq(
      ("1", "c", "cc", Date.valueOf(day2), Date.valueOf(day2)),
      ("2", "b", "bb", Date.valueOf(day2), Date.valueOf(day2)),
      ("3", "e", "ee", Date.valueOf(day2), Date.valueOf(day2)),
      ("4", "d", "dd", Date.valueOf(day2), Date.valueOf(day2))
    ).toDF("id", "value", ds.oid, ingestedOnNane, validFromName)

    DeltaLoader
      .scd2(scd2Output, "default", "scd2table", updates, Seq("id"),
        buidName, oidName, isCurrentName, List(), DELTA.sparkFormat, validFromName, validToName, Map(), maxDate)

    val updates2: DataFrame = Seq(
      ("1", "c", "cc", Date.valueOf(day3), Date.valueOf(day3)),
      ("2", "b", "bb", Date.valueOf(day3), Date.valueOf(day3)),
      ("3", "e", "ee", Date.valueOf(day3), Date.valueOf(day3)),
      ("4", "d", "dd", Date.valueOf(day3), Date.valueOf(day3))
    ).toDF("id", "value", ds.oid, ingestedOnNane, validFromName)

    DeltaLoader
      .scd2(scd2Output, "default", "scd2table", updates2, Seq("id"),
        buidName, oidName, isCurrentName, List(), DELTA.sparkFormat, validFromName, validToName, Map(), maxDate)

    //since day2 and day3 are the same, no data should have day3 as 'valid_from' date
    //all current rows should have 'valid_to' date set to 9999-12-31
    // 1 should be updated by day2 load
    // 2 should remain unchanged
    // 3 was added on day2 and update the same day thus only the last version of the same day remains
    // 4 was added on day2 with no original value from day 1

    val finalDf = DeltaLoader.read(scd2Output, DELTA.sparkFormat, Map(), Some("default"), Some("scd2table")).as[Scd2Table]
    finalDf.count() shouldBe 5
    finalDf
      .collect() should contain allElementsOf Seq(
      Scd2Table("1", "356a192b7913b04c54574d18c28d46e6395428ab", "a", "aa", Date.valueOf("2020-01-01"), Date.valueOf("2020-01-01"), Date.valueOf("2020-01-01"), false),
      Scd2Table("1", "356a192b7913b04c54574d18c28d46e6395428ab", "c", "cc", Date.valueOf("2020-01-02"), Date.valueOf("2020-01-02"), Date.valueOf("9999-12-31"), true ),
      Scd2Table("2", "da4b9237bacccdf19c0760cab7aec4a8359010b0", "b", "bb", Date.valueOf("2020-01-01"), Date.valueOf("2020-01-01"), Date.valueOf("9999-12-31"), true ),
      Scd2Table("3", "77de68daecd823babbb58edb1c8e14d7106e83bb", "e", "ee", Date.valueOf("2020-01-02"), Date.valueOf("2020-01-02"), Date.valueOf("9999-12-31"), true ),
      Scd2Table("4", "1b6453892473a467d07372d45eb05abc2031647a", "d", "dd", Date.valueOf("2020-01-02"), Date.valueOf("2020-01-02"), Date.valueOf("9999-12-31"), true ),
    )

  }

  "overwrite" should "replace all data" in {

    import spark.implicits._

    val day1 = Date.valueOf("1900-01-01")
    val day2 = Date.valueOf("1900-01-02")
    val day3 = Date.valueOf("1900-01-03")

    val existing: DataFrame = Seq(
      ("1", day1),
      ("2", day2)
    ).toDF("id", "ingested_on")

    val updates: DataFrame = Seq(
      ("3", day2),
      ("4", day2),
      ("5", day3)
    ).toDF("id", "ingested_on")

    DeltaLoader.writeOnce(testtableoverwite, "default", "testtableoverwite", existing, List("ingested_on"), "delta")
    spark.table("testtableoverwite").show(false)

    DeltaLoader.overwritePartition(testtableoverwite, "default", "testtableoverwite", updates, List("ingested_on"), "delta")

    spark.table("testtableoverwite").as[(String, Date)].collect() should contain allElementsOf Seq(
      ("3", day2),
      ("4", day2),
      ("5", day3)
    )

  }

  "overwrite partition" should "replace partition" in {

    import spark.implicits._

    val day1 = Date.valueOf("1900-01-01")
    val day2 = Date.valueOf("1900-01-02")
    val day3 = Date.valueOf("1900-01-03")
    val tableName = "testtableoverwite"

    val existing: DataFrame = Seq(
      ("1", day1),
      ("2", day2)
    ).toDF("id", "ingested_on")

    val updates: DataFrame = Seq(
      ("3", day2),
      ("4", day2),
      ("5", day3)
    ).toDF("id", "ingested_on")

    DeltaLoader.writeOnce(testtableoverwite, "default", tableName, existing, List("ingested_on"), "delta")
    spark.table(tableName).show(false)

    DeltaLoader.overwritePartition(testtableoverwite, "default", tableName, updates, List("ingested_on"), "delta")

    spark.table(tableName).as[(String, Date)].collect() should contain allElementsOf Seq(
      ("1", day1),
      ("3", day2),
      ("4", day2),
      ("5", day3)
    )

  }

  "insert" should "add rows to existing rows" in {

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)
    val tableName = "inserttable"

    val existing = Seq(
      TestData("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    )

    DeltaLoader.writeOnce(output, "default", tableName, existing.toDF(), List(), "delta", Map("mergeSchema" -> "true"))

    val updates: Seq[TestData] = Seq(
      TestData("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    )
    val updatedDF = updates.toDF

    val expectedResult: Seq[TestData] = existing ++ updates

    DeltaLoader.insert(
      output,
      "default",
      tableName,
      updatedDF,
      List(),
      "delta",
      Map()
    )

    DeltaTable
      .forName(tableName)
      .toDF.as[TestData].collect() should contain allElementsOf expectedResult

  }

  "upsert" should "update existing data and insert new data" in {

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)
    val tableName = "upserttable"

    val existing: DataFrame = Seq(
      TestData("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaLoader.writeOnce(output, "default", tableName, existing, List(), "delta")

    val updates: Seq[TestData] = Seq(
      TestData("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    )
    val updatedDF = updates.toDF

    val expectedResult: Seq[TestData] = updates ++ Seq(TestData("aaaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1))

    DeltaLoader.upsert(
      output,
      "default",
      tableName,
      updatedDF,
      Seq("uid"),
      List(),
      "delta",
      Map()
    )

    DeltaTable
      .forName(tableName)
      .toDF.as[TestData].collect() should contain allElementsOf expectedResult

  }

}
