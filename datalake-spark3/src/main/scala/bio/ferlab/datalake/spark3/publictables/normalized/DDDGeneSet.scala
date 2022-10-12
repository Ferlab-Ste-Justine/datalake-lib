package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.utils.Coalesce

class DDDGeneSet()(implicit conf: Configuration) extends ETLP {
  private val ddd_gene_set = conf.getDataset("raw_ddd_gene_set")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_ddd_gene_set")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      ddd_gene_set.id -> ddd_gene_set.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(ddd_gene_set.id)
      .select(
        $"gene symbol" as "symbol",
        $"gene mim" as "omim_gene_id",
        $"disease name" as "disease_name",
        $"disease mim" as "disease_omim_id",
        $"DDD category" as "ddd_category",
        $"mutation consequence" as "mutation_consequence",
        split($"phenotypes", ";") as "phenotypes",
        split($"organ specificity list", ";") as "organ_specificity",
        $"panel",
        $"hgnc id" as "hgnc_id"
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
