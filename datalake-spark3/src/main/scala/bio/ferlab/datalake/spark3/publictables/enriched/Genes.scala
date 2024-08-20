package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.SparkUtils.removeEmptyObjectsIn
import bio.ferlab.datalake.spark3.publictables.enriched.Genes._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

case class Genes(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  val mainDestination: DatasetConf = conf.getDataset("enriched_genes")
  val omim_gene_set: DatasetConf = conf.getDataset("normalized_omim_gene_set")
  val orphanet_gene_set: DatasetConf = conf.getDataset("normalized_orphanet_gene_set")
  val hpo_gene_set: DatasetConf = conf.getDataset("normalized_hpo_gene_set")
  val human_genes: DatasetConf = conf.getDataset("normalized_human_genes")
  val ddd_gene_set: DatasetConf = conf.getDataset("normalized_ddd_gene_set")
  val cosmic_gene_set: DatasetConf = conf.getDataset("normalized_cosmic_gene_set")
  val gnomad_constraint: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(
      omim_gene_set.id -> omim_gene_set.read,
      orphanet_gene_set.id -> orphanet_gene_set.read,
      hpo_gene_set.id -> hpo_gene_set.read,
      human_genes.id -> human_genes.read,
      ddd_gene_set.id -> ddd_gene_set.read,
      cosmic_gene_set.id -> cosmic_gene_set.read,
      gnomad_constraint.id -> gnomad_constraint.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val humanGenes = data(human_genes.id)
      .select($"chromosome", $"symbol", $"entrez_gene_id", $"omim_gene_id",
        $"external_references.hgnc" as "hgnc",
        $"ensembl_gene_id",
        $"map_location" as "location",
        $"description" as "name",
        $"synonyms" as "alias",
        regexp_replace($"type_of_gene", "-", "_") as "biotype")

    humanGenes
      .withOrphanet(data(orphanet_gene_set.id))
      .withHPO(data(hpo_gene_set.id))
      .withOmim(data(omim_gene_set.id))
      .withDDD(data(ddd_gene_set.id))
      .withCosmic(data(cosmic_gene_set.id))
      .withGnomadConstraint(data(gnomad_constraint.id))
  }

  override def defaultRepartition: DataFrame => DataFrame = Coalesce()
}

object Genes {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    Genes(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)

  implicit class DataFrameOps(df: DataFrame) {

    def joinAndMergeWith(other: DataFrame,
                         joinOn: Seq[String],
                         asColumnName: String,
                         aggFirst: Boolean = false,
                         broadcastOtherDf: Boolean = false): DataFrame = {
      val aggFn: Column => Column = c => if (aggFirst) first(c) else collect_list(c)
      val aggDF = df
        .join(if (broadcastOtherDf) broadcast(other) else other, joinOn, "left")
        .groupBy("symbol")
        .agg(
          first(struct(df("*"))) as "hg",
          aggFn(struct(other.drop(joinOn: _*)("*"))) as asColumnName,
        )
        .select(col("hg.*"), col(asColumnName))
      if (aggFirst)
        aggDF
      else
        aggDF.withColumn(asColumnName, removeEmptyObjectsIn(asColumnName))
    }

    def withGnomadConstraint(gnomad: DataFrame): DataFrame = {
      val gnomadConstraint = gnomad
        .groupBy("chromosome", "symbol")
        .agg(
          max("pLI") as "pli",
          max("oe_lof_upper") as "loeuf"
        )
      df.joinAndMergeWith(gnomadConstraint, Seq("chromosome", "symbol"), "gnomad", aggFirst = true, broadcastOtherDf = true)
    }

    def withOrphanet(orphanet: DataFrame): DataFrame = {
      val orphanetPrepared = orphanet
        .select(col("gene_symbol") as "symbol", col("disorder_id"), col("name") as "panel", col("type_of_inheritance") as "inheritance")

      df.joinAndMergeWith(orphanetPrepared, Seq("symbol"), "orphanet", broadcastOtherDf = true)
    }

    def withOmim(omim: DataFrame): DataFrame = {
      val omimPrepared = omim.where(col("phenotype.name").isNotNull)
        .select(
          col("omim_gene_id"),
          col("phenotype.name") as "name",
          col("phenotype.omim_id") as "omim_id",
          col("phenotype.inheritance") as "inheritance",
          col("phenotype.inheritance_code") as "inheritance_code")
      df.joinAndMergeWith(omimPrepared, Seq("omim_gene_id"), "omim", broadcastOtherDf = true)
    }

    def withDDD(ddd: DataFrame): DataFrame = {
      val dddPrepared = ddd.select("disease_name", "symbol")
      df.joinAndMergeWith(dddPrepared, Seq("symbol"), "ddd", broadcastOtherDf = true)
    }

    def withCosmic(cosmic: DataFrame): DataFrame = {
      val cosmicPrepared = cosmic.select("symbol", "tumour_types_germline")
      df.joinAndMergeWith(cosmicPrepared, Seq("symbol"), "cosmic", broadcastOtherDf = true)
    }

    def withHPO(hpo: DataFrame): DataFrame = {
      val hpoPrepared = hpo.select(col("entrez_gene_id"), col("hpo_term_id"), col("hpo_term_name"))
        .distinct()
        .withColumn("hpo_term_label", concat(col("hpo_term_name"), lit(" ("), col("hpo_term_id"), lit(")")))
      df.joinAndMergeWith(hpoPrepared, Seq("entrez_gene_id"), "hpo", broadcastOtherDf = true)
    }
  }
}

