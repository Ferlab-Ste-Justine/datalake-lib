package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.time.LocalDateTime

case class EnsemblMapping(rc: RuntimeETLContext) extends SimpleETLP(rc)  {
  override val mainDestination: DatasetConf = conf.getDataset("normalized_ensembl_mapping")

  val raw_ensembl_entrez: DatasetConf = conf.getDataset("raw_ensembl_entrez")
  val raw_ensembl_refseq: DatasetConf = conf.getDataset("raw_ensembl_refseq")
  val raw_ensembl_uniprot: DatasetConf = conf.getDataset("raw_ensembl_uniprot")
  val raw_ensembl_ena: DatasetConf = conf.getDataset("raw_ensembl_ena")
  val raw_ensembl_gff: DatasetConf = conf.getDataset("raw_ensembl_gtf")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(
      raw_ensembl_entrez.id -> raw_ensembl_entrez.read,
      raw_ensembl_refseq.id -> raw_ensembl_refseq.read,
      raw_ensembl_uniprot.id -> raw_ensembl_uniprot.read,
      raw_ensembl_ena.id -> raw_ensembl_ena.read,
      raw_ensembl_gff.id -> raw_ensembl_gff.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {

    val refseq = data(raw_ensembl_refseq.id).renameIds.renameExternalReference("refseq")
    val entrez = data(raw_ensembl_entrez.id).renameIds.renameExternalReference("entrez")
    val uniprot = data(raw_ensembl_uniprot.id).renameIds.renameExternalReference("uniprot")
    val ena = data(raw_ensembl_ena.id).renameIds.withColumnRenamed("taxid", "tax_id")

    //  gff file contains tags used to identify MANE and Canonical transcripts.
    //  Historically these tags were taken from the canonical dataset, but this file is not always present.
    val gff = data(raw_ensembl_gff.id).filter(
      Seq("Ensembl_canonical", "MANE_Select", "MANE_Plus_Clinical")
        .map(value => col("tag").contains(value))
        .reduce(_ || _)
    ).select(
      col("transcript_id").as("ensembl_transcript_id"),
      col("tag"),
    ).withColumn("tag", explode(functions.split(col("tag"), ",").cast(ArrayType(StringType))))

    val joinedDf = ena
      .join(refseq, Seq("ensembl_gene_id", "ensembl_transcript_id"), "left")
      .join(entrez, Seq("ensembl_gene_id", "ensembl_transcript_id"), "left")
      .join(uniprot, Seq("ensembl_gene_id", "ensembl_transcript_id"), "left")
      .join(gff, Seq("ensembl_transcript_id"), "left")

    joinedDf
      .groupBy("ensembl_gene_id", "ensembl_transcript_id")
      .agg(
        collect_set(col("tag")).as("tags"),
        externalIDs(List("refseq", "entrez", "uniprot")) :+
          first("species").as("species") :+
          first("tax_id").as("tax_id") :+
          collect_set("primary_accession").as("primary_accessions") :+
          collect_set("secondary_accession").as("secondary_accessions"): _*
      )
      .withColumn("refseq_mrna_id", filter(col("refseq"), c => c("id").like("NM_%"))(0)("id"))
      .withColumn("refseq_protein_id", filter(col("refseq"), c => c("id").like("NP_%"))(0)("id"))
      .withColumn(
        "is_canonical",
        when(array_contains(col("tags"), "Ensembl_canonical"), lit(true)).otherwise(lit(false))
      )
      .withColumn(
        "is_mane_select",
        when(array_contains(col("tags"), "MANE_Select"), lit(true)).otherwise(lit(false))
      )
      .withColumn(
        "is_mane_plus",
        when(array_contains(col("tags"), "MANE_Plus_Clinical"), lit(true))
          .otherwise(lit(false))
      )
      .withColumn("genome_build", lit("GRCh38"))
  }

  override def defaultRepartition: DataFrame => DataFrame = Coalesce()

  private val externalIDs: List[String] => List[Column] =
    _.map(externalDb =>
      collect_set(
        struct(
          col(s"${externalDb}_id") as "id",
          col(s"${externalDb}_database") as "database"
        )
      ) as externalDb
    )

  implicit class DataFrameOps(df: DataFrame) {

    def renameIds: DataFrame = {
      df.withColumnRenamed("gene_stable_id", "ensembl_gene_id")
        .withColumnRenamed("transcript_stable_id", "ensembl_transcript_id")
        .withColumnRenamed("protein_stable_id", "ensembl_protein_id")
    }

    def renameExternalReference(prefix: String): DataFrame = {
      df.withColumnRenamed("xref", s"${prefix}_id")
        .withColumnRenamed("db_name", s"${prefix}_database")
    }
  }
}

object EnsemblMapping {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    EnsemblMapping(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
