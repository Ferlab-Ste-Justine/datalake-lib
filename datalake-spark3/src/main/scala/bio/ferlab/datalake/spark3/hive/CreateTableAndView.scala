package bio.ferlab.datalake.spark3.hive

import bio.ferlab.datalake.commons.config.{Configuration, RuntimeETLContext}
import mainargs.{ParserForMethods, arg, main}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

case class CreateTableAndView(rc: RuntimeETLContext, datasetIds: Seq[String]) {
  implicit val conf: Configuration = rc.config
  implicit val spark: SparkSession = rc.spark

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  def run(): Unit = {
    val (successes, failures) = datasetIds.map(id => Try(conf.getDataset(id))).partition(_.isSuccess)
    failures.foreach(f => log.warn(f.failed.get.getMessage)) // Log dataset ids that didn't match

    val datasetConfs = successes.map(_.get)
    datasetConfs.foreach { ds =>
      log.info(s"CREATING: [${ds.id}]")
      // Create table
      ds.table match {
        case Some(tableConf) =>
          val createDatabaseSql = s"CREATE DATABASE IF NOT EXISTS ${tableConf.database}"
          log.info(s"Creating database of the table using SQL: $createDatabaseSql")
          spark.sql(createDatabaseSql)

          val createTableSql = s"CREATE TABLE IF NOT EXISTS ${tableConf.fullName} USING ${ds.format.sparkFormat} LOCATION '${ds.location}'"
          log.info(s"Creating table using SQL: $createTableSql")
          spark.sql(createTableSql)

          // Create view after table creation
          ds.view match {
            case Some(viewConf) =>
              val createDatabaseSql = s"CREATE DATABASE IF NOT EXISTS ${viewConf.database}"
              log.info(s"Creating database of the view using SQL: $createDatabaseSql")
              spark.sql(createDatabaseSql)

              val createViewSql = s"CREATE VIEW IF NOT EXISTS ${viewConf.fullName} AS SELECT * FROM ${tableConf.fullName}"
              log.info(s"Creating view using SQL: $createViewSql")
              spark.sql(createViewSql)
            case None =>
              log.info(s"No view configuration defined. Can't create view of dataset [${ds.id}].")
          }

        case None =>
          log.warn(s"No table configuration defined. Can't create table of dataset [${ds.id}].")
      }
    }
  }
}

object CreateTableAndView {
  @main
  def run(rc: RuntimeETLContext, @arg(name = "dataset_id", short = 'd', doc = "Dataset id") datasetIds: Seq[String]): Unit = {
    CreateTableAndView(rc, datasetIds).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
