package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.Format.CSV
import bio.ferlab.datalake.commons.config.{DatasetConf, LoadType, TableConf}
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import scala.util.Try

class DatasetConfImplicitsSpec extends AnyFlatSpec with WithSparkSession with GivenWhenThen with Matchers with BeforeAndAfterAll {


  import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._

  val tableName = "my_table"
  val databaseName = "default"

  import spark.implicits._

  val output: String = getClass.getClassLoader.getResource("normalized/").getFile + "testtable"

  override def beforeAll(): Unit = {
    Try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
      spark.sql(s"DROP TABLE IF EXISTS $databaseName.$tableName")
    }
  }

  def init(): Unit = withOutputFolder("output") { output =>
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

    init()
    val dsConf = DatasetConf("id", "storageid", "path", CSV, LoadType.Read, table = Some(TableConf(databaseName, tableName)))
    assert(dsConf.tableExist)

  }

  it should "return false if table does not exist in spark catalog" in {
    init()
    val dsConf = DatasetConf("id", "storageid", "path", CSV, LoadType.Read, table = Some(TableConf(databaseName, "another_table")))
    assert(!dsConf.tableExist)
  }
  it should "return false if schema does not exist in spark catalog" in {
    init()
    val dsConf = DatasetConf("id", "storageid", "path", CSV, LoadType.Read, table = Some(TableConf("another_schema",tableName)))
    assert(!dsConf.tableExist)
  }

}
