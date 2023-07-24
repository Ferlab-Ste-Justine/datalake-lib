package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class DBNSFPRaw(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {
  override val mainDestination: DatasetConf = conf.getDataset("normalized_dbnsfp")
  val raw_dbnsfp: DatasetConf = conf.getDataset("raw_dbnsfp")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(raw_dbnsfp.id -> raw_dbnsfp.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(raw_dbnsfp.id)
      .withColumnRenamed("#chr", "chromosome")
      .withColumnRenamed("position_1-based", "start")
      .withColumnRenamed("ref", "reference")
      .withColumnRenamed("alt", "alternate")
  }


}

object DBNSFPRaw {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    DBNSFPRaw(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
