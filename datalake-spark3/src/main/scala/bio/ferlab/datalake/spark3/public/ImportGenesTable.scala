package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}
import bio.ferlab.datalake.spark3.etl.ETL
import bio.ferlab.datalake.spark3.implicits.SparkUtils.removeEmptyObjectsIn
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class ImportGenesTable()(implicit conf: Configuration)
  extends ETL()(conf) {

  val destination       : SourceConf = conf.getSource("genes")
  val omim_gene_set     : SourceConf = conf.getSource("omim_gene_set")
  val orphanet_gene_set : SourceConf = conf.getSource("orphanet_gene_set")
  val hpo_gene_set      : SourceConf = conf.getSource("hpo_gene_set")
  val human_genes       : SourceConf = conf.getSource("human_genes")
  val ddd_gene_set      : SourceConf = conf.getSource("ddd_gene_set")
  val cosmic_gene_set   : SourceConf = conf.getSource("cosmic_gene_set")

  override def extract()(implicit spark: SparkSession): Map[SourceConf, DataFrame] = {
    Map(
      omim_gene_set     -> spark.read.parquet(omim_gene_set.location),
      orphanet_gene_set -> spark.read.parquet(orphanet_gene_set.location),
      hpo_gene_set      -> spark.read.parquet(hpo_gene_set.location),
      human_genes       -> spark.read.parquet(human_genes.location),
      ddd_gene_set      -> spark.read.parquet(ddd_gene_set.location),
      cosmic_gene_set   -> spark.read.parquet(cosmic_gene_set.location)
    )
  }

  override def transform(data: Map[SourceConf, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val humanGenes = data(human_genes)
      .select($"chromosome", $"symbol", $"entrez_gene_id", $"omim_gene_id",
        $"external_references.hgnc" as "hgnc",
        $"ensembl_gene_id",
        $"map_location" as "location",
        $"description" as "name",
        $"synonyms" as "alias",
        regexp_replace($"type_of_gene", "-", "_") as "biotype")

    val orphanet = data(orphanet_gene_set)
      .select($"gene_symbol" as "symbol", $"disorder_id", $"name" as "panel", $"type_of_inheritance" as "inheritance")

    val omim = data(omim_gene_set)
      .where($"phenotype.name".isNotNull)
      .select(
        $"omim_gene_id",
        $"phenotype.name" as "name",
        $"phenotype.omim_id" as "omim_id",
        $"phenotype.inheritance" as "inheritance",
        $"phenotype.inheritance_code" as "inheritance_code")

    val hpo = data(hpo_gene_set)
      .select($"entrez_gene_id", $"hpo_term_id", $"hpo_term_name")
      .distinct()
      .withColumn("hpo_term_label", concat($"hpo_term_name", lit(" ("), $"hpo_term_id", lit(")")))

    val ddd_gene_set_df = data(ddd_gene_set)
      .select("disease_name", "symbol")

    val cosmic_gene_set_df = data(cosmic_gene_set)
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

  override def load(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    data
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format(destination.format.sparkFormat)
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
    data
  }
}


