package bio.ferlab.datalake.testutils

import org.scalatest.BeforeAndAfterAll

trait CreateDatabasesBeforeAll extends BeforeAndAfterAll {
  this: SparkSpec =>

  val dbToCreate: List[String]

  override def beforeAll(): Unit = {
    dbToCreate.foreach(d => spark.sql(s"CREATE DATABASE IF NOT EXISTS $d"))
    super.beforeAll()
  }
}
