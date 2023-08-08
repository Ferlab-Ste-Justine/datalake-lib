package bio.ferlab.datalake.testutils

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.commons.file.HadoopFileSystem
import org.scalatest.BeforeAndAfterAll

import scala.util.Try

trait CleanUpBeforeAll extends BeforeAndAfterAll {
  this: SparkSpec =>

  implicit val conf: Configuration
  val dsToClean: List[DatasetConf]

  override def beforeAll(): Unit = {
    dsToClean.foreach { ds =>
      ds.table.foreach(t => spark.sql(s"DROP TABLE IF EXISTS ${t.fullName}"))
      Try(HadoopFileSystem.remove(ds.location))
    }
    super.beforeAll()
  }
}
