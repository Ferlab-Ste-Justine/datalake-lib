package bio.ferlab.datalake.spark3.datastore

import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class HiveSqlBinderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val tableName = "test_hive_binder"
  val databaseName = "default"
  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtable"


  import spark.implicits._

  override def beforeAll(): Unit = {
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${databaseName}")
      spark.sql(s"DROP TABLE IF EXISTS $databaseName.$tableName")
      new File(output).delete()
    }
  }

  "truncate" should "remove all data in the table and keep the schema" in {
    Seq(
      ("1", "test"),
      ("2", "test2")
    ).toDF()
      .write
      .mode("overwrite")
      .format("parquet")
      .option("path", output)
      .saveAsTable(s"$databaseName.$tableName")

    HiveSqlBinder.truncate(output, databaseName, tableName, List(), "parquet")
    spark.table(s"$databaseName.$tableName").count() shouldBe 0
  }

  "drop" should "remove all data in the table and remove the metadata" in {
    Seq(
      ("1", "test"),
      ("2", "test2")
    ).toDF()
      .write
      .mode("overwrite")
      .format("parquet")
      .option("path", output)
      .saveAsTable(s"$databaseName.$tableName")

    HiveSqlBinder.drop(output, databaseName, tableName, LOCAL)
    assertThrows[AnalysisException](spark.table(s"$databaseName.$tableName"))
  }

}
