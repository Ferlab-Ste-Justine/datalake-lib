package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.commons.utils.Coalesce
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.removeEmptyObjectsIn
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class Genes()(implicit conf: Configuration) extends ETLSingleDestination {

  val mainDestination       : DatasetConf = conf.getDataset("enriched_genes")
  val omim_gene_set     : DatasetConf = conf.getDataset("normalized_omim_gene_set")
  val orphanet_gene_set : DatasetConf = conf.getDataset("normalized_orphanet_gene_set")
  val hpo_gene_set      : DatasetConf = conf.getDataset("normalized_hpo_gene_set")
  val human_genes       : DatasetConf = conf.getDataset("normalized_human_genes")
  val ddd_gene_set      : DatasetConf = conf.getDataset("normalized_ddd_gene_set")
  val cosmic_gene_set   : DatasetConf = conf.getDataset("normalized_cosmic_gene_set")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      omim_gene_set.id     -> omim_gene_set.read,
      orphanet_gene_set.id -> orphanet_gene_set.read,
      hpo_gene_set.id      -> hpo_gene_set.read,
      human_genes.id       -> human_genes.read,
      ddd_gene_set.id      -> ddd_gene_set.read,
      cosmic_gene_set.id   -> cosmic_gene_set.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime,
                         currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val humanGenes = data(human_genes.id)
      .select($"chromosome", $"symbol", $"entrez_gene_id", $"omim_gene_id",
        $"external_references.hgnc" as "hgnc",
        $"ensembl_gene_id",
        $"map_location" as "location",
        $"description" as "name",
        $"synonyms" as "alias",
        regexp_replace($"type_of_gene", "-", "_") as "biotype")

    val orphanet = data(orphanet_gene_set.id)
      .select($"gene_symbol" as "symbol", $"disorder_id", $"name" as "panel", $"type_of_inheritance" as "inheritance")

    val omim = data(omim_gene_set.id)
      .where($"phenotype.name".isNotNull)
      .select(
        $"omim_gene_id",
        $"phenotype.name" as "name",
        $"phenotype.omim_id" as "omim_id",
        $"phenotype.inheritance" as "inheritance",
        $"phenotype.inheritance_code" as "inheritance_code")

    val hpo = data(hpo_gene_set.id)
      .select($"entrez_gene_id", $"hpo_term_id", $"hpo_term_name")
      .distinct()
      .withColumn("hpo_term_label", concat($"hpo_term_name", lit(" ("), $"hpo_term_id", lit(")")))

    val ddd_gene_set_df = data(ddd_gene_set.id)
      .select("disease_name", "symbol")

    val cosmic_gene_set_df = data(cosmic_gene_set.id)
      .select("symbol", "tumour_types_germline")

    humanGenes
      .joinAndMergeWith(orphanet, Seq("symbol"), "orphanet")
      .joinAndMergeWith(hpo, Seq("entrez_gene_id"), "hpo")
      .joinAndMergeWith(omim, Seq("omim_gene_id"), "omim")
      .joinAndMergeWith(ddd_gene_set_df, Seq("symbol"), "ddd")
      .joinAndMergeWith(cosmic_gene_set_df, Seq("symbol"), "cosmic")

  }

  implicit class DataFrameOps(df: DataFrame) {
    def joinAndMergeWith(gene_set: DataFrame, joinOn: Seq[String], asColumnName: String): DataFrame = {
      df
        .join(gene_set, joinOn, "left")
        .groupBy("symbol")
        .agg(
          first(struct(df("*"))) as "hg",
          collect_list(struct(gene_set.drop(joinOn:_*)("*"))) as asColumnName,
        )
        .select(col("hg.*"), col(asColumnName))
        .withColumn(asColumnName, removeEmptyObjectsIn(asColumnName))
    }
  }

  override def defaultRepartition: DataFrame => DataFrame = Coalesce()
}


