package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.genomics.Splits._
import bio.ferlab.datalake.spark3.genomics.{FrequencySplit, OccurrenceSplit}
import bio.ferlab.datalake.spark3.genomics.enriched.Variants._
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.{locus, locusColumnNames}
import bio.ferlab.datalake.spark3.implicits.SparkUtils.firstAs
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

/**
 * This ETL create an aggregated table on occurrences of SNV variants. Occurrences are aggregated by calculating the frequencies specified in parameter frequencies.
 * The table is enriched with information from other datasets such as genes, dbsnp, clinvar, 1000 genomes, topmed_bravo, gnomad_genomes_v2, gnomad_exomes_v2, gnomad_genomes_v3.
 *
 * @param participantId     column used to distinct participants in order to calculate total number of participants (pn) and total allele number (an)
 * @param affectedStatus    column used to calculate frequencies for affected / unaffected participants
 * @param snvDatasetId      the id of the dataset containing the SNV variants
 * @param frequencies       the frequencies to calculate. See [[FrequencyOperations.freq]]
 * @param extraAggregations extra aggregations to be computed when grouping occurrences by locus. Will be added to the root of the data
 * @param spliceAi          bool indicating whether or not to join variants with SpliceAI. Defaults to true.
 * @param rc                the etl context
 */
case class Variants(rc: RuntimeETLContext, participantId: Column = col("participant_id"),
                    affectedStatus: Column = col("affected_status"), filterSnv: Option[Column] = Some(col("has_alt")),
                    snvDatasetId: String, splits: Seq[OccurrenceSplit], extraAggregations: Seq[Column] = Nil,
                    checkpoint: Boolean = false, spliceAi: Boolean = true, destinationDataSetId: String = "enriched_variants") extends SimpleSingleETL(rc) {
  override val mainDestination: DatasetConf = conf.getDataset(destinationDataSetId)
  if (checkpoint) {
    spark.sparkContext.setCheckpointDir(s"${mainDestination.rootPath}/checkpoints")
  }
  protected val thousand_genomes: DatasetConf = conf.getDataset("normalized_1000_genomes")
  protected val topmed_bravo: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  protected val gnomad_genomes_v2: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v2_1_1")
  protected val gnomad_exomes_v2: DatasetConf = conf.getDataset("normalized_gnomad_exomes_v2_1_1")
  protected val gnomad_genomes_v3: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")
  protected val gnomad_genomes_v4: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v4")
  protected val dbsnp: DatasetConf = conf.getDataset("normalized_dbsnp")
  protected val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  protected val genes: DatasetConf = conf.getDataset("enriched_genes")
  protected val cosmic: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")
  protected val spliceai_indel: DatasetConf = conf.getDataset("enriched_spliceai_indel")
  protected val spliceai_snv: DatasetConf = conf.getDataset("enriched_spliceai_snv")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      thousand_genomes.id -> thousand_genomes.read,
      topmed_bravo.id -> topmed_bravo.read,
      gnomad_genomes_v2.id -> gnomad_genomes_v2.read,
      gnomad_exomes_v2.id -> gnomad_exomes_v2.read,
      gnomad_genomes_v3.id -> gnomad_genomes_v3.read,
      dbsnp.id -> dbsnp.read,
      clinvar.id -> clinvar.read,
      genes.id -> genes.read,
      cosmic.id -> cosmic.read,
      spliceai_indel.id -> (if (spliceAi) spliceai_indel.read else spark.emptyDataFrame),
      spliceai_snv.id -> (if (spliceAi) spliceai_snv.read else spark.emptyDataFrame),
      snvDatasetId -> conf.getDataset(snvDatasetId).read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val snv = filterSnv.map(f => data(snvDatasetId).where(f)).getOrElse(data(snvDatasetId))
    val variantAggregations: Seq[Column] = Seq(
      firstAs("hgvsg", ignoreNulls = true),
      firstAs("genes_symbol", ignoreNulls = true),
      firstAs("name", ignoreNulls = true),
      firstAs("end", ignoreNulls = true),
      firstAs("variant_class", ignoreNulls = true),
    ) ++ extraAggregations
    val variants = snv
      .groupByLocus()
      .agg(variantAggregations.head, variantAggregations.tail: _*)
      .withColumn("dna_change", concat_ws(">", col("reference"), col("alternate")))
      .withColumn("assembly_version", lit("GRCh38"))

    val variantsCheckpoint = if (checkpoint) variants.checkpoint() else variants

    variantsCheckpoint
      .withFrequencies(participantId, affectedStatus, snv, splits, checkpoint)
      .withPopulations(data(thousand_genomes.id), data(topmed_bravo.id), data(gnomad_genomes_v2.id), data(gnomad_exomes_v2.id), data(gnomad_genomes_v3.id), data(gnomad_genomes_v4.id))
      .withDbSNP(data(dbsnp.id))
      .withClinvar(data(clinvar.id))
      .withGenes(data(genes.id))
      .withCosmic(data(cosmic.id))
      .withSpliceAi(snv = data(spliceai_snv.id), indel = data(spliceai_indel.id), compute = spliceAi)
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
        exists($"genes", gene => gene("omim").isNotNull and size(gene("omim")) > 0) -> "OMIM",
        exists($"genes", gene => gene("ddd").isNotNull and size(gene("ddd")) > 0) -> "DDD",
        exists($"genes", gene => gene("cosmic").isNotNull and size(gene("cosmic")) > 0) -> "Cosmic",
        exists($"genes", gene => gene("gnomad").isNotNull) -> "gnomAD",
        exists($"genes", gene => gene("spliceai").isNotNull) -> "SpliceAI",
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
        $"clinvar".isNotNull -> "Clinvar",
        $"cmc".isNotNull -> "Cosmic",
      )
      val dfWithVariantExternalReference = conditionValueMap.foldLeft {
        df.withColumn(outputColumn, when($"rsnumber".isNotNull, array(lit("DBSNP"))).otherwise(array()))
      } { case (d, (condition, value)) => d
        .withColumn(outputColumn,
          when(condition, array_union(col(outputColumn), array(lit(value)))).otherwise(col(outputColumn)))
      }
      // Only for CLIN at the moment
      if (dfWithVariantExternalReference.columns.contains("pubmed")) {
        dfWithVariantExternalReference.withColumn(outputColumn,
          when($"pubmed".isNotNull, array_union(col(outputColumn), array(lit("PubMed")))).otherwise(col(outputColumn)))
      } else dfWithVariantExternalReference
    }


    def withPopulations(
                         thousandGenomes: DataFrame,
                         topmed: DataFrame,
                         gnomadGenomesV2: DataFrame,
                         gnomadExomesV2: DataFrame,
                         gnomadGenomesV3: DataFrame,
                         gnomadGenomesV4: DataFrame)(implicit spark: SparkSession): DataFrame = {
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
      val shapedGnomadGenomesV4 = gnomadGenomesV4.selectLocus($"ac", $"af", $"an", $"hom")

      df
        .joinAndMerge(shapedThousandGenomes, "thousand_genomes", "left")
        .joinAndMerge(shapedTopmed, "topmed_bravo", "left")
        .joinAndMerge(shapedGnomadGenomesV2, "gnomad_genomes_2_1_1", "left")
        .joinAndMerge(shapedGnomadExomesV2, "gnomad_exomes_2_1_1", "left")
        .joinAndMerge(shapedGnomadGenomesV3, "gnomad_genomes_3", "left")
        .joinAndMerge(shapedGnomadGenomesV4, "gnomad_genomes_4", "left")
        .select(df("*"),
          struct(
            col("thousand_genomes"),
            col("topmed_bravo"),
            col("gnomad_genomes_2_1_1"),
            col("gnomad_exomes_2_1_1"),
            col("gnomad_genomes_3"),
            col("gnomad_genomes_4")) as "external_frequencies")
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


    def withFrequencies(participantId: Column, affectedStatus: Column, snv: DataFrame, splits: Seq[OccurrenceSplit], checkpoint: Boolean = false): DataFrame = splits match {
      case Nil => df
      case _ =>
        val variantWithFreq = snv.split(participantId = participantId, affectedStatus = affectedStatus, splits)
        if (checkpoint) {
          df.joinByLocus(variantWithFreq.checkpoint(), "left").checkpoint()
        } else {
          df.joinByLocus(variantWithFreq, "left")
        }
    }

    def withCosmic(cosmic: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      val w = Window.partitionBy(locus: _*).orderBy($"sample_mutated".desc)

      val cmc = cosmic.selectLocus(
          $"mutation_url",
          $"shared_aa",
          $"genomic_mutation_id" as "cosmic_id",
          $"cosmic_sample_mutated" as "sample_mutated",
          $"cosmic_sample_tested" as "sample_tested",
          $"mutation_significance_tier" as "tier",
          $"cosmic_sample_mutated".divide($"cosmic_sample_tested") as "sample_ratio"
        )
        // Deduplicate
        .withColumn("rn", row_number().over(w))
        .filter($"rn" === 1)
        .drop("rn")

      df.joinAndMerge(cmc, "cmc", "left")
    }

    def withSpliceAi(snv: DataFrame, indel: DataFrame, compute: Boolean = true)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      def joinAndMergeIntoGenes(variants: DataFrame, spliceai: DataFrame): DataFrame = {
        if (!variants.isEmpty) {
          variants
            .select($"*", explode_outer($"genes") as "gene", $"gene.symbol" as "symbol") // explode_outer since genes can be null
            .join(spliceai, locusColumnNames :+ "symbol", "left")
            .drop("symbol") // only used for joining
            .withColumn("gene", struct($"gene.*", $"spliceai")) // add spliceai struct as nested field of gene struct
            .groupByLocus()
            .agg(
              first(struct(variants.drop("genes")("*"))) as "variant",
              collect_list("gene") as "genes" // re-create genes list for each locus, now containing spliceai struct
            )
            .select("variant.*", "genes")
        } else variants
      }

       if (compute) {
         val spliceAiSnvPrepared = snv
           .selectLocus($"symbol", $"max_score" as "spliceai")

         val spliceAiIndelPrepared = indel
           .selectLocus($"symbol", $"max_score" as "spliceai")

         val snvVariants = df
           .where($"variant_class" === "SNV")

         val otherVariants = df
           .where($"variant_class" =!= "SNV")

         val snvVariantsWithSpliceAi = joinAndMergeIntoGenes(snvVariants, spliceAiSnvPrepared)
         val otherVariantsWithSpliceAi = joinAndMergeIntoGenes(otherVariants, spliceAiIndelPrepared)

         snvVariantsWithSpliceAi.unionByName(otherVariantsWithSpliceAi, allowMissingColumns = true)
       } else {
         // Add empty spliceai struct
         df
           .withColumn("genes", transform($"genes", g => g.withField("spliceai", lit(null).cast(
             StructType(Seq(
              StructField("ds", DoubleType),
               StructField("type", ArrayType(StringType))
             ))))))
       }
    }
  }
}

