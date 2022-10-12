package bio.ferlab.datalake.spark3.publictables.normalized.refseq

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class RefSeqAnnotation()(implicit configuration: Configuration) extends ETLP {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_refseq_annotation")
  val raw_refseq_annotation: DatasetConf = conf.getDataset("raw_refseq_annotation")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_refseq_annotation.id -> raw_refseq_annotation.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val original = data(raw_refseq_annotation.id)

    val regions = original
      .where($"type" === "region" and $"genome" === "chromosome")
      .select("seqId", "chromosome")

    original
      .drop("chromosome")
      .join(regions, Seq("seqId"))
      .repartition(3)
  }

}

