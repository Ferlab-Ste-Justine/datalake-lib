package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.testutils.SparkSpec
import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import com.dimafeng.testcontainers.{JdbcDatabaseContainer, PostgreSQLContainer}
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName

import java.sql.Timestamp
import java.time.LocalDateTime

class JdbcLoaderSpec extends SparkSpec with Matchers with TestContainerForEach {

  import spark.implicits._

  val databaseName = "test_db"
  val schemaName = "test_schema"
  val tableName = "test"

  override val containerDef: PostgreSQLContainer.Def = PostgreSQLContainer.Def(
    dockerImageName = DockerImageName.parse("postgres:15.1"),
    databaseName = databaseName,
    username = "scala",
    password = "scala",
    commonJdbcParams = JdbcDatabaseContainer.CommonParams(initScriptPath = Some("jdbc/init-dbt.sql"))
  )

  def withPostgresContainer(options: Map[String, String] => Unit): Unit = withContainers { psqlContainer =>
    options(Map(
      "url" -> psqlContainer.jdbcUrl,
      "driver" -> "org.postgresql.Driver",
      "user" -> psqlContainer.username,
      "password" -> psqlContainer.password
    ))
  }

  "upsert" should "update existing data and insert new data" in {
    withPostgresContainer { options =>
      val readOptions = options + ("dbtable" -> s"$schemaName.$tableName")
      val day1 = LocalDateTime.of(2020, 1, 1, 1, 1, 1)
      val day2 = day1.plusDays(1)

      // Write existing data in database
      val existingData = Seq(
        TestData(`uid` = "a", `oid` = "a", `chromosome` = "1", `createdOn` = Timestamp.valueOf(day1), `updatedOn` = Timestamp.valueOf(day1), `data` = 1),
        TestData(`uid` = "aa", `oid` = "aa", `chromosome` = "2", `createdOn` = Timestamp.valueOf(day1), `updatedOn` = Timestamp.valueOf(day1), `data` = 1),
      )
      JdbcLoader.writeOnce("", schemaName, tableName, existingData.toDF(), List(), "jdbc", options)

      // Check existing data was written in the database
      val existingDfInPsql = JdbcLoader.read("", "jdbc", readOptions, Some(schemaName), Some(tableName))
      existingDfInPsql
        .as[TestData]
        .collect() should contain theSameElementsAs existingData

      // Upsert data
      val upsertData = Seq(
        TestData(`uid` = "aa", `oid` = "aa", `chromosome` = "1", `createdOn` = Timestamp.valueOf(day1), `updatedOn` = Timestamp.valueOf(day2), `data` = 2), // update
        TestData(`uid` = "aaa", `oid` = "aaa", `chromosome` = "3", `createdOn` = Timestamp.valueOf(day2), `updatedOn` = Timestamp.valueOf(day2), `data` = 2), // insert
      )
      val upsertResult = JdbcLoader.upsert("", schemaName, tableName, upsertData.toDF(), primaryKeys = Seq("uid", "oid"), List(), "jdbc", options)
      val updatedDfInPsql = JdbcLoader.read("", "jdbc", readOptions, Some(schemaName), Some(tableName))

      val expectedData = Seq(
        TestData(`uid` = "a", `oid` = "a", `chromosome` = "1", `createdOn` = Timestamp.valueOf(day1), `updatedOn` = Timestamp.valueOf(day1), `data` = 1),
        TestData(`uid` = "aa", `oid` = "aa", `chromosome` = "1", `createdOn` = Timestamp.valueOf(day1), `updatedOn` = Timestamp.valueOf(day2), `data` = 2),
        TestData(`uid` = "aaa", `oid` = "aaa", `chromosome` = "3", `createdOn` = Timestamp.valueOf(day2), `updatedOn` = Timestamp.valueOf(day2), `data` = 2),
      )

      upsertResult
        .as[TestData]
        .collect() should contain theSameElementsAs expectedData

      updatedDfInPsql
        .as[TestData]
        .collect() should contain theSameElementsAs expectedData
    }
  }
}
