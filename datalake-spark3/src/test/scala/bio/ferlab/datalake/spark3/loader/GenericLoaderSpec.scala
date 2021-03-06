package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class GenericLoaderSpec extends AnyFlatSpec with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "test_generic"

  val tableName = "test_generic"
  val databaseName = "default"

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

}
