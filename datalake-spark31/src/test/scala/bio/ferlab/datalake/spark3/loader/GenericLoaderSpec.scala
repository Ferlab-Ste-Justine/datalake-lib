package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime

class GenericLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "test_generic"

  val tableName = "test_generic"
  val databaseName = "default"

  override def beforeAll(): Unit = {
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${databaseName}")
      spark.sql(s"DROP TABLE IF EXISTS ${tableName}")
      new File(output).delete()
    }
  }

  "insert" should "add new data" in {

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: Seq[TestData] = Seq(
      TestData("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    )



    GenericLoader.writeOnce(output, databaseName, tableName, existing.toDF, List("uid"), "parquet")

    val updates: Seq[TestData] = Seq(
      TestData("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aaa", "aaa", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    )
    val updatedDF = updates.toDF

    val expectedResult: Seq[TestData] = updates ++ existing

    GenericLoader.insert(
      output,
      databaseName,
      tableName,
      updatedDF,
      List("uid"),
      "parquet"
    )

    spark.read.parquet(output).as[TestData].collect() should contain allElementsOf expectedResult

  }

  "upsert" should "insert or update data" in {

    import spark.implicits._

    val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
    val day2 = day1.plusDays(1)

    val existing: DataFrame = Seq(
      TestData("a", "a", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    ).toDF

    GenericLoader.upsert(output, databaseName, tableName, existing, List("uid"), List(), "parquet")

    val updatesDf: DataFrame = Seq(
      TestData("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2)
    ).toDF

    val expectedResult: Seq[TestData] = Seq(
      TestData("a", "b", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aa", "bb", Timestamp.valueOf(day2), Timestamp.valueOf(day2), 2),
      TestData("aaa", "aaa", Timestamp.valueOf(day1), Timestamp.valueOf(day1), 1)
    )

    GenericLoader.upsert(
      output,
      databaseName,
      tableName,
      updatesDf,
      List("uid"),
      List(),
      "parquet"
    )

    spark.read.parquet(output).as[TestData].collect() should contain allElementsOf expectedResult

  }

}