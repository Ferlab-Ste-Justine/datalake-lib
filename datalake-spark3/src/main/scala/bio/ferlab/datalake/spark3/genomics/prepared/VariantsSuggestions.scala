package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import bio.ferlab.datalake.spark3.implicits.SparkUtils.{getColumnOrElse, getColumnOrElseArray}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

class VariantsSuggestions(override implicit val conf: Configuration)
  extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_variant_suggestions")
  private val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  private val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")

  final val variantSymbolAaChangeWeight = 4
  final val variantSymbolWeight = 2

  final val indexColumns =
    List("type", "locus", "suggestion_id", "hgvsg", "suggest", "chromosome", "rsnumber", "symbol_aa_change")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      enriched_variants.id -> enriched_variants.read,
      enriched_consequences.id -> enriched_consequences.read
    )

  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {

    val variants =
      data(enriched_variants.id)
        .selectLocus(
          col("hgvsg"),
          col("rsnumber"),
          col("clinvar.clinvar_id") as "clinvar_id")

    val consequences = data(enriched_consequences.id)
      .selectLocus(
        col("symbol"),
        col("aa_change"),
        col("ensembl_gene_id"),
        col("ensembl_transcript_id"),
        col("refseq_mrna_id"),
        col("refseq_protein_id")
      )
      .dropDuplicates()

    getVariantSuggest(variants, consequences)
  }


  def getVariantSuggest(variants: DataFrame, consequence: DataFrame): DataFrame = {
    val groupedByLocusConsequences = consequence
      .withColumn("symbol_aa_change", concat_ws(" ", col("symbol"), col("aa_change")))
      .withColumn("ensembl_gene_id", getColumnOrElse("ensembl_gene_id"))
      .withColumn("ensembl_transcript_id", getColumnOrElse("ensembl_transcript_id"))
      .withColumn("refseq_mrna_id", getColumnOrElseArray("refseq_mrna_id"))
      .withColumn("refseq_protein_id", getColumnOrElse("refseq_protein_id"))
      .groupBy(locus: _*)
      .agg(
        array_remove(collect_set(col("aa_change")), "") as "aa_change",
        array_remove(collect_set(col("symbol_aa_change")), "") as "symbol_aa_change",
        collect_set(col("ensembl_gene_id")) as "ensembl_gene_id",
        collect_set(col("ensembl_transcript_id")) as "ensembl_transcript_id",
        array_distinct(flatten(collect_list(col("refseq_mrna_id")))) as "refseq_mrna_id",
        collect_set(col("refseq_protein_id")) as "refseq_protein_id"
      )

    variants
      .withColumn("clinvar_id", getColumnOrElse("clinvar_id"))
      .withColumn("hgvsg", getColumnOrElse("hgvsg"))
      .withColumn("rsnumber", getColumnOrElse("rsnumber"))
      .joinByLocus(groupedByLocusConsequences, "left")
      .withColumn("type", lit("variant"))
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn(
        "suggestion_id",
        sha1(col("locus"))
      ) //this maps to `hash` column in variant_centric index
      .withColumn("hgvsg", col("hgvsg"))
      .withColumn(
        "suggest",
        array(
          struct(
            array_remove(
              flatten(
                array(
                  array(col("hgvsg")),
                  array(col("locus")),
                  array(col("rsnumber")),
                  array(col("clinvar_id"))
                )
              ),
              ""
            ) as "input",
            lit(variantSymbolAaChangeWeight) as "weight"
          ),
          struct(
            array_distinct(
              array_remove(
                flatten(
                  array(
                    col("aa_change"),
                    col("symbol_aa_change"),
                    col("ensembl_gene_id"),
                    col("ensembl_transcript_id"),
                    col("refseq_mrna_id"),
                    col("refseq_protein_id")
                  )
                ),
                ""
              )
            ) as "input",
            lit(variantSymbolWeight) as "weight"
          )
        )
      )
      .select(indexColumns.head, indexColumns.tail: _*)
  }
}
