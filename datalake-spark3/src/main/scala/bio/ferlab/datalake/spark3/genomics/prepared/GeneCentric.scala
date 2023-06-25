package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.functions.{col, sha1}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class GeneCentric(override implicit val conf: Configuration) extends ETLSingleDestination() {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_gene_centric")
  private val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(enriched_genes.id -> enriched_genes.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data(enriched_genes.id)
      .withColumn("hash", sha1(col("symbol")))
  }


}
