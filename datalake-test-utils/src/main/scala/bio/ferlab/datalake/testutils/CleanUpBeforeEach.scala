package bio.ferlab.datalake.testutils

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.commons.file.HadoopFileSystem
import org.scalatest.BeforeAndAfterEach

import scala.util.Try

trait CleanUpBeforeEach extends BeforeAndAfterEach {
  this: SparkSpec =>

  implicit val conf: Configuration
  val dsToClean: List[DatasetConf]

  override def beforeEach(): Unit = {
    dsToClean.foreach { ds =>
      ds.table.foreach(t => spark.sql(s"DROP TABLE IF EXISTS ${t.fullName}"))
      Try(HadoopFileSystem.remove(ds.location))
    }
    super.beforeEach()
  }
}
