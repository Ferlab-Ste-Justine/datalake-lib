package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.spark3.etl.Partitioning
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

case class Test(uid: String, oid: String,
                createdOn: Timestamp, updatedOn: Timestamp,
                data: Long, chromosome: String = "1", start: Long = 666)

class DeltaLoaderSpec extends AnyFlatSpec with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.ui.enabled", value = false)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtable"

  "upsert" should "update existing data and insert new data" in {

    spark.sql("DROP TABLE IF EXISTS default.testtable")

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      Test("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      Test("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaLoader.writeOnce(output, "default", "testtable", existing, Partitioning.default)

    val updates: Seq[Test] = Seq(
      Test("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    )
    val updatedDF = updates.toDF

    val expectedResult: Seq[Test] = updates

    DeltaLoader.upsert(
      output,
      "default",
      "testtable",
      updatedDF,
      Seq("uid"),
      partitioning = Partitioning.default
    )

    DeltaTable
      .forName("testtable")
      .toDF.as[Test].collect() should contain allElementsOf expectedResult

  }

  "scd1" should "update existing data and insert new data" in {

    spark.sql("DROP TABLE IF EXISTS default.testtable")

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      Test("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      Test("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    DeltaLoader.writeOnce(output, "default", "testtable", existing, Partitioning.default)

    val updates: DataFrame = Seq(
      Test("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      Test("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    ).toDF

    val expectedResult: Seq[Test] = Seq(
      Test("a", "b", Timestamp.valueOf(day1), Timestamp.valueOf(day2), 2),   //updated only will be updated
      Test("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2), //will be inserted
      Test("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)//will stay the same
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
      Partitioning.default
    )

    DeltaTable
      .forName("testtable")
      .toDF.as[Test].collect() should contain allElementsOf expectedResult

  }

}
