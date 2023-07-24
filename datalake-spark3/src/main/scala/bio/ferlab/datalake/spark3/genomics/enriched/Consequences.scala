package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.etl.RuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.formatted_consequences
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.sql.Timestamp
import java.time.LocalDateTime

case class Consequences(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_consequences")
  val normalized_consequences: DatasetConf = conf.getDataset("normalized_consequences")
  val dbnsfp_original: DatasetConf = conf.getDataset("enriched_dbnsfp")
  val normalized_ensembl_mapping: DatasetConf = conf.getDataset("normalized_ensembl_mapping")
  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      normalized_consequences.id -> normalized_consequences.read
        .where(col("updated_on") >= Timestamp.valueOf(lastRunDateTime)),
      dbnsfp_original.id -> dbnsfp_original.read,
      normalized_ensembl_mapping.id -> normalized_ensembl_mapping.read,
      enriched_genes.id -> enriched_genes.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    val consequences = data(normalized_consequences.id)

    val ensembl_mapping = data(normalized_ensembl_mapping.id)
      .withColumn("uniprot_id", col("uniprot")(0)("id"))
      .select(
        $"ensembl_transcript_id",
        $"ensembl_gene_id",
        $"uniprot_id",
        array($"refseq_mrna_id") as "ensembl_refseq_mrna_id",
        $"refseq_protein_id",
        $"is_mane_select" as "mane_select",
        $"is_mane_plus" as "mane_plus",
        $"is_canonical")

    val chromosomes = consequences.select("chromosome").distinct().as[String].collect()

    val dbnsfp = data(dbnsfp_original.id).where(col("chromosome").isin(chromosomes: _*))

    val csq = consequences
      .drop("batch_id", "name", "end", "hgvsg", "variant_class", "ensembl_regulatory_id", "study_id")
      .withColumn("consequence", formatted_consequences)
      .drop("consequences")
      .withColumnRenamed("impact", "vep_impact")

    joinWithDBNSFP(csq, dbnsfp)
      .join(ensembl_mapping, Seq("ensembl_transcript_id", "ensembl_gene_id"), "left")
      .withColumn("refseq_mrna_id", coalesce(col("refseq_mrna_id"), col("ensembl_refseq_mrna_id")))
      .withColumn("mane_plus", coalesce(col("mane_plus"), lit(false)))
      .withColumn("mane_select", coalesce(col("mane_select"), lit(false)))
      .withColumn("canonical", coalesce(col("is_canonical"), lit(false)))
      .drop("ensembl_refseq_mrna_id")
      .drop("is_canonical")
      .withPickedCsqPerLocus(data(enriched_genes.id))
  }

  def joinWithDBNSFP(csq: DataFrame, dbnsfp: DataFrame): DataFrame = {
    import spark.implicits._
    val dbnsfpRenamed =
      dbnsfp
        .filter($"aaref".isNotNull)
        .withColumn("start", col("start").cast(LongType))
        .selectLocus(
          $"ensembl_transcript_id" as "ensembl_feature_id",
          struct(
            $"SIFT_score" as "sift_score",
            $"SIFT_pred" as "sift_pred",
            $"Polyphen2_HVAR_score" as "polyphen2_hvar_score",
            $"Polyphen2_HVAR_pred" as "polyphen2_hvar_pred",
            $"FATHMM_score" as "fathmm_score",
            $"FATHMM_pred" as "fathmm_pred",
            $"CADD_raw" as "cadd_score",
            $"CADD_phred" as "cadd_phred",
            $"DANN_score" as "dann_score",
            $"REVEL_score" as "revel_score",
            $"LRT_score" as "lrt_score",
            $"LRT_pred" as "lrt_pred") as "predictions",
          struct($"phyloP17way_primate", $"phyloP100way_vertebrate") as "conservations",
        )

    csq
      .join(dbnsfpRenamed, Seq("chromosome", "start", "reference", "alternate", "ensembl_feature_id"), "left")
      .select(csq("*"), dbnsfpRenamed("predictions"), dbnsfpRenamed("conservations"))
      .withColumn(mainDestination.oid, col("created_on"))

  }
}

object Consequences {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    Consequences(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

