package bio.ferlab.datalake.spark3.loader

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

class DeltaLoaderSpec extends AnyFlatSpec with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val testtableoverwite: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtableoverwite"
  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtable"

  "overwrite" should "replace all data" in {

    spark.sql("DROP TABLE IF EXISTS default.testtable")

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

    spark.sql("DROP TABLE IF EXISTS default.testtable")

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
      ("1", day1),
      ("3", day2),
      ("4", day2),
      ("5", day3)
    )

  }

  "upsert" should "update existing data and insert new data" in {

    spark.sql("DROP TABLE IF EXISTS default.testtable")

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      TestData("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaLoader.writeOnce(output, "default", "testtable", existing, List(), "delta")

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
      "testtable",
      updatedDF,
      Seq("uid"),
      List(),
      "delta",
      Map()
    )

    DeltaTable
      .forName("testtable")
      .toDF.as[TestData].collect() should contain allElementsOf expectedResult

  }

  "scd1" should "update existing data and insert new data" in {

    spark.sql("DROP TABLE IF EXISTS default.testtable")

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      TestData("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaLoader
      .scd1(
        output,
        "default",
        "testtable",
        existing,
        Seq("uid"),
        "oid",
        "createdOn",
        "updatedOn",
        List(),
        "delta"
      )

    val updates: DataFrame = Seq(
      TestData("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    ).toDF

    val expectedResult: Seq[TestData] = Seq(
      TestData("a", "b", Timestamp.valueOf(day1), Timestamp.valueOf(day2), 2),   //updated only will be updated
      TestData("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2), //will be inserted
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)//will stay the same
    )

    DeltaLoader.scd1(
      output,
      "default",
      "testtable",
      updates,
      Seq("uid"),
      "oid",
      "createdOn",
      "updatedOn",
      List(),
      "delta"
    )

    DeltaTable
      .forName("testtable")
      .toDF.as[TestData].collect() should contain allElementsOf expectedResult

  }

}
