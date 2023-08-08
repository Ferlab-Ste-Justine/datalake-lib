package bio.ferlab.datalake.spark3.datastore

import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.testutils.{CreateDatabasesBeforeAll, SparkSpec}
import org.apache.spark.sql.AnalysisException

import java.io.File
import scala.util.Try

class HiveSqlBinderSpec extends SparkSpec with CreateDatabasesBeforeAll {

  val tableName = "test_hive_binder"
  val databaseName = "default"
  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtable"

  import spark.implicits._

  override val dbToCreate: List[String] = List(databaseName)

  override def beforeAll(): Unit = {
    Try {
      spark.sql(s"DROP TABLE IF EXISTS `$databaseName`.`$tableName`")
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
