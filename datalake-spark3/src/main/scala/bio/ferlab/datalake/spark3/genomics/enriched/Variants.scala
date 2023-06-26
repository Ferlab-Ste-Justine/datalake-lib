package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.genomics.Frequencies._
import bio.ferlab.datalake.spark3.genomics.FrequencySplit
import bio.ferlab.datalake.spark3.genomics.enriched.Variants._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import bio.ferlab.datalake.spark3.implicits.SparkUtils
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 * This ETL create an aggregated table on occurrences of SNV variants. Occurrences are aggregated by calculating the frequencies specified in parameter frequencies.
 * The table is enriched with information from other datasets such as genes, dbsnp, clinvar, spliceai, 1000 genomes, topmed_bravo, gnomad_genomes_v2, gnomad_exomes_v2, gnomad_genomes_v3.
 *
 * @param participantId  column used to distinct participants in order to calculate total number of participants (pn) and total allele number (an)
 * @param affectedStatus column used to calculate frequencies for affected / unaffected participants
 * @param snvDatasetId   the id of the dataset containing the SNV variants
 * @param frequencies    the frequencies to calculate. See [[FrequencyOperations.freq]]
 * @param configuration  the configuration object
 */
class Variants(participantId: Column = col("participant_id"), affectedStatus: Column = col("affected_status"), filterSnv: Option[Column] = Some(col("has_alt")), snvDatasetId: String, frequencies: Seq[FrequencySplit])(implicit configuration: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_variants")
  protected val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  protected val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  protected val gnomad_genomes_v2: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v2_1_1")
  protected val gnomad_exomes_v2: DatasetConf = conf.getDataset("normalized_gnomad_exomes_v2_1_1")
  protected val gnomad_genomes_v3: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")
  protected val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  protected val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  protected val genes: DatasetConf = conf.getDataset("enriched_genes")
  protected val spliceai: DatasetConf = conf.getDataset("enriched_spliceai")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      thousand_genomes.id -> thousand_genomes.read,
      topmed_bravo.id -> topmed_bravo.read,
      gnomad_genomes_v2.id -> gnomad_genomes_v2.read,
      gnomad_exomes_v2.id -> gnomad_exomes_v2.read,
      gnomad_genomes_v3.id -> gnomad_genomes_v3.read,
      dbsnp.id -> dbsnp.read,
      clinvar.id -> clinvar.read,
      genes.id -> genes.read,
      spliceai.id -> spliceai.read,
      snvDatasetId -> conf.getDataset(snvDatasetId).read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    val snv = filterSnv.map(f => data(snvDatasetId).where(f)).getOrElse(data(snvDatasetId))
    val variants = snv.selectLocus(col("hgvsg"), col("genes_symbol"), col("name"), col("end"), col("variant_class"))
      .groupByLocus()
      .agg(firstAs("hgvsg", ignoreNulls = true), firstAs("genes_symbol", ignoreNulls = true), firstAs("name", ignoreNulls = true), firstAs("end", ignoreNulls = true), firstAs("variant_class", ignoreNulls = true))
      .withColumn("dna_change", concat_ws(">", col("reference"), col("alternate")))
      .withColumn("assembly_version", lit("GRCh38"))

    variants
      .withFrequencies(participantId, affectedStatus, snv, frequencies)
      .withPopulations(data(thousand_genomes.id), data(topmed_bravo.id), data(gnomad_genomes_v2.id), data(gnomad_exomes_v2.id), data(gnomad_genomes_v3.id))
      .withDbSNP(data(dbsnp.id))
      .withClinvar(data(clinvar.id))
      .withGenes(data(genes.id))
      .withSpliceAi(data(spliceai.id))
      .withGeneExternalReference
      .withVariantExternalReference
      .withColumn("locus", concat_ws("-", locus: _*))
      .withColumn("hash", sha1(col("locus")))
      .drop("genes_symbol")
  }


}

object Variants {
  implicit class DataFrameOps(df: DataFrame) {
    def withGeneExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumn = "gene_external_reference"

      val conditionValueMap: List[(Column, String)] = List(
        exists($"genes", gene => gene("orphanet").isNotNull and size(gene("orphanet")) > 0) -> "Orphanet",
        exists($"genes", gene => gene("omim").isNotNull and size(gene("omim")) > 0) -> "OMIM"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumn, when(exists($"genes", gene => gene("hpo").isNotNull and size(gene("hpo")) > 0), array(lit("HPO"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumn,
          when(condition, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
    }

    def withVariantExternalReference(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val outputColumn = "variant_external_reference"

      val conditionValueMap: List[(Column, String)] = List(
        $"clinvar".isNotNull -> "Clinvar"
      )
      conditionValueMap.foldLeft {
        df.withColumn(outputColumn, when($"rsnumber".isNotNull, array(lit("DBSNP"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumn,
          when(condition, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
    }


    def withPopulations(
                         thousandGenomes: DataFrame,
                         topmed: DataFrame,
                         gnomadGenomesV2: DataFrame,
                         gnomadExomesV2: DataFrame,
                         gnomadGenomesV3: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val shapedThousandGenomes = thousandGenomes
        .selectLocus($"ac".cast("long"), $"af", $"an".cast("long"))
      val shapedTopmed = topmed
        .selectLocus(
          $"ac".cast("long"),
          $"af",
          $"an".cast("long"),
          $"homozygotes".cast("long") as "hom",
          $"heterozygotes".cast("long") as "het")

      val shapedGnomadGenomesV2 = gnomadGenomesV2.selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
      val shapedGnomadExomesV2 = gnomadExomesV2.selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"hom".cast("long"))
      val shapedGnomadGenomesV3 = gnomadGenomesV3.selectLocus($"ac".cast("long"), $"af", $"an".cast("long"), $"nhomalt".cast("long") as "hom")

      df
        .joinAndMerge(shapedThousandGenomes, "thousand_genomes", "left")
        .joinAndMerge(shapedTopmed, "topmed_bravo", "left")
        .joinAndMerge(shapedGnomadGenomesV2, "gnomad_genomes_2_1_1", "left")
        .joinAndMerge(shapedGnomadExomesV2, "gnomad_exomes_2_1_1", "left")
        .joinAndMerge(shapedGnomadGenomesV3, "gnomad_genomes_3", "left")
        .select(df("*"),
          struct(
            col("thousand_genomes"),
            col("topmed_bravo"),
            col("gnomad_genomes_2_1_1"),
            col("gnomad_exomes_2_1_1"),
            col("gnomad_genomes_3")) as "external_frequencies")
    }

    def withDbSNP(dbsnp: DataFrame): DataFrame = {
      //We first take rsnumber from variants.name, and then from dbsnp if variants.name is null
      df
        .joinByLocus(dbsnp, "left")
        .select(df.drop("name")("*"), coalesce(df("name"), dbsnp("name")) as "rsnumber")
    }

    def withClinvar(clinvar: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      df
        .joinAndMerge(
          clinvar.selectLocus($"name" as "clinvar_id", $"clin_sig", $"conditions", $"inheritance", $"interpretations"),
          "clinvar",
          "left")
    }

    def withGenes(genes: DataFrame): DataFrame = {
      df
        .join(genes, df("chromosome") === genes("chromosome") && array_contains(df("genes_symbol"), genes("symbol")), "left")
        .drop(genes("chromosome"))
        .groupByLocus()
        .agg(
          first(struct(df("*"))) as "variant",
          collect_list(struct(genes.drop("chromosome")("*"))) as "genes"
        )
        .select("variant.*", "genes")
    }


    def withFrequencies(participantId: Column, affectedStatus: Column, snv: DataFrame, frequencies: Seq[FrequencySplit]): DataFrame = frequencies match {
      case Nil => df
      case _ =>
        val variantWithFreq = snv.freq(participantId = participantId, affectedStatus = affectedStatus, split = frequencies)
        df.joinByLocus(variantWithFreq, "inner")
    }

    def withSpliceAi(spliceai: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val scores = spliceai.selectLocus($"symbol", $"max_score" as "spliceai")
        .withColumn("type", when($"spliceai.ds" === 0, null).otherwise($"spliceai.type"))
        .withColumn("spliceai", struct($"spliceai.ds" as "ds", $"type"))
        .drop("type")

      df
        .select($"*", explode_outer($"genes") as "gene", $"gene.symbol" as "symbol") // explode_outer since genes can be null
        .join(scores, locusColumnNames :+ "symbol", "left")
        .drop("symbol") // only used for joining
        .withColumn("gene", struct($"gene.*", $"spliceai")) // add spliceai struct as nested field of gene struct
        .groupByLocus()
        .agg(
          first(struct(df.drop("genes")("*"))) as "variant",
          collect_list("gene") as "genes" // re-create genes list for each locus, now containing spliceai struct
        )
        .select("variant.*", "genes")
    }
  }
}

