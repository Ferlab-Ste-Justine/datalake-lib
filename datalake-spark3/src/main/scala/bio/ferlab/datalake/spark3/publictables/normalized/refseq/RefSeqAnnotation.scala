package bio.ferlab.datalake.spark3.publictables.normalized.refseq

import bio.ferlab.datalake.commons.config.{DatasetConf, FixedRepartition}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.etl.{ETLContext, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class RefSeqAnnotation(rc: ETLContext) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val raw_refseq_annotation: DatasetConf = conf.getDataset("raw_refseq_annotation")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      raw_refseq_annotation.id -> raw_refseq_annotation.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val original = data(raw_refseq_annotation.id)

    val regions = original
      .where($"type" === "region" and $"genome" === "chromosome")
      .select("seqId", "chromosome")

    original
      .drop("chromosome")
      .join(regions, Seq("seqId"))
  }

  override val defaultRepartition: DataFrame => DataFrame = FixedRepartition(3)

}

object RefSeqAnnotation {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    RefSeqAnnotation(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

