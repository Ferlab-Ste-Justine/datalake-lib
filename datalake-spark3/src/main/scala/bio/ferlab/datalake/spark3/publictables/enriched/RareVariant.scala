package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class RareVariant(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_rare_variant")
  val gnomad: DatasetConf = conf.getDataset("normalized_gnomad_joint_v4")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(
      gnomad.id -> gnomad.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    data(gnomad.id)
      .select(columns.locus :+ col("af_joint"): _*)
      .groupByLocus()
      .agg(max("af_joint") as "af")
      .withColumn("is_rare", col("af") <= 0.01)
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(60))

}

object RareVariant {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    RareVariant(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
