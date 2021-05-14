package bio.ferlab.datalake.spark3.implicits

import io.projectglow.Glow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DecimalType, DoubleType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.net.URI

object SparkUtils {

  implicit class SparkUtilsOperations(df: DataFrame) {
    def withCombinedFrequencies(combined: String, prefix1: String, prefix2: String): DataFrame = {
      df
        .withColumn(s"${combined}_ac", col(s"${prefix1}_ac") + col(s"${prefix2}_ac"))
        .withColumn(s"${combined}_an", col(s"${prefix1}_an") + col(s"${prefix2}_an"))
        .withColumn(s"${combined}_af", (col(s"${combined}_ac") / col(s"${combined}_an")).cast(DecimalType(38, 18)) )
        .withColumn(s"${combined}_heterozygotes", col(s"${prefix1}_heterozygotes") + col(s"${prefix2}_heterozygotes"))
        .withColumn(s"${combined}_homozygotes", col(s"${prefix1}_homozygotes") + col(s"${prefix2}_homozygotes"))
    }
  }

  val filename: Column = regexp_extract(input_file_name(), ".*/(.*)", 1)

  /**
   * Check if the hadoop file exists
   *
   * @param path  Path to check. Accept some patterns
   * @param spark session that contains hadoop config
   * @return
   */
  def fileExist(path: String)(implicit spark: SparkSession): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = if (path.startsWith("s3a")) {
      val bucket = path.replace("s3a://", "").split("/").head
      org.apache.hadoop.fs.FileSystem.get(new URI(s"s3a://$bucket"), conf)
    } else {
      org.apache.hadoop.fs.FileSystem.get(conf)
    }

    val statuses = fs.globStatus(new Path(path))
    statuses != null && statuses.nonEmpty
  }

  @Deprecated
  def allFilesPath(input: String): String = s"$input/*.filtered.deNovo.vep.vcf.gz"

  def vcf(files: List[String])(implicit spark: SparkSession): DataFrame = vcf(files.mkString(","))

  def vcf(input: String)(implicit spark: SparkSession): DataFrame = {
    val inputs = input.split(",")
    val df = spark.read.format("vcf")
      .option("flattenInfoFields", "true")
      .load(inputs: _*)
      .withColumnRenamed("filters", "INFO_FILTERS") // Avoid losing filters columns before split

    Glow.transform("split_multiallelics", df)
  }

  def tableName(table: String, studyId: String, releaseId: String): String = {
    s"${table}_${studyId.toLowerCase}_${releaseId.toLowerCase}"
  }

  def tableName(table: String, studyId: String, releaseId: String, database: String = "variant"): String = {
    s"${database}.${table}_${studyId.toLowerCase}_${releaseId.toLowerCase}"
  }

  def colFromArrayOrField(df: DataFrame, colName: String): Column = {
    df.schema(colName).dataType match {
      case ArrayType(_, _) => df(colName)(0)
      case _ => df(colName)
    }
  }

  def union(df1: DataFrame, df2: DataFrame)(implicit spark: SparkSession) = (df1, df2)
  match {
    case (p, c) if p.isEmpty => c
    case (p, c) if c.isEmpty => p
    case (p, c) => p.union(c)
    case _ => spark.emptyDataFrame
  }

  def firstAs(c: String): Column = first(col(c)) as c

  object columns {
    val chromosome: Column = ltrim(col("contigName"), "chr") as "chromosome"
    val reference: Column = col("referenceAllele") as "reference"
    val start: Column = (col("start") + 1) as "start"
    val end: Column = (col("end") + 1) as "end"
    val alternate: Column = col("alternateAlleles")(0) as "alternate"
    val name: Column = col("names")(0) as "name"

    val calculated_duo_af: String => Column = duo => {
      val ac = col(s"${duo}_ac")
      val an = col(s"${duo}_an")
      val af = when(an === 0, 0)
        .otherwise(ac / an)
      when(af.isNull, 0).otherwise(af).cast(DoubleType)
    }

    val calculated_af: Column = {
      val ac = col("ac")
      val an = col("an")
      val af = when(an === 0, 0)
        .otherwise(ac / an)
      when(af.isNull, 0).otherwise(af).cast(DoubleType)
    }

    val calculated_af_from_an: Column => Column = an => {
      val ac = col("ac")
      val af = when(an === 0, 0)
        .otherwise(ac / an)
      when(af.isNull, 0).otherwise(af).cast(DoubleType)
    }



    val ac: Column = col("INFO_AC")(0) as "ac"
    val af: Column = (col("INFO_AF")(0) as "af").cast(DoubleType)
    val an: Column = col("INFO_AN") as "an"

    val afr_af: Column = (col("INFO_AFR_AF")(0) as "afr_af").cast(DoubleType)
    val eur_af: Column = (col("INFO_EUR_AF")(0) as "eur_af").cast(DoubleType)
    val sas_af: Column = (col("INFO_SAS_AF")(0) as "sas_af").cast(DoubleType)
    val amr_af: Column = (col("INFO_AMR_AF")(0) as "amr_af").cast(DoubleType)
    val eas_af: Column = (col("INFO_EAS_AF")(0) as "eas_af").cast(DoubleType)

    val dp: Column = col("INFO_DP") as "dp"

    val familyVariantWindow: WindowSpec = Window.partitionBy("chromosome", "start", "reference", "alternate", "family_id")

    val familyCalls: Column = map_from_entries(collect_list(struct(col("participant_id"), col("calls"))).over(familyVariantWindow))

    /**
     * has_alt return 1 if there is at least one alternative allele. Note : It cannot returned a boolean beacause it's used to partition data.
     * It looks like Glue does not support partition by boolean
     * */
    val has_alt: Column = when(array_contains(col("genotype.calls"), 1), 1).otherwise(0) as "has_alt"

    val calculated_ac: Column = when(col("zygosity") === "HET", 1)
      .when(col("zygosity") === "HOM", 2)
      .otherwise(0) as "ac"

    val calculate_an_upper_bound_kf: Long => Column = pc => lit(pc * 2) as "an_upper_bound_kf"
    val calculate_an_lower_bound_kf: Column =
      when(col("zygosity") === "HOM" or col("zygosity") === "HET" or col("zygosity") === "WT", 2)
        .otherwise(0) as "an_lower_bound_kf"

    val homozygotes: Column = when(col("zygosity") === "HOM", 1).otherwise(0) as "homozygotes"
    val heterozygotes: Column = when(col("zygosity") === "HET", 1).otherwise(0) as "heterozygotes"

    val zygosity: Column => Column = c => when(c(0) === 1 && c(1) === 1, "HOM")
      .when(c(0) === 0 && c(1) === 1, "HET")
      .when(c(0) === 0 && c(1) === 0, "WT")
      .when(c(0) === 1 && c(1) === 0, "HET")
      .when(c.isNull, lit(null).cast("string"))
      .otherwise("UNK")

    //Annotations
    val annotations: Column = when(col("splitFromMultiAllelic"), expr("filter(INFO_ANN, ann-> ann.Allele == alternateAlleles[0])")).otherwise(col("INFO_ANN")) as "annotations"
    val csq: Column = when(col("splitFromMultiAllelic"), expr("filter(INFO_CSQ, ann-> ann.Allele == alternateAlleles[0])")).otherwise(col("INFO_CSQ")) as "annotations"
    val firstAnn: Column = annotations.getItem(0) as "annotation"
    val firstCsq: Column = csq.getItem(0) as "annotation"
    val consequences: Column = col("annotation.Consequence") as "consequences"
    val impact: Column = col("annotation.IMPACT") as "impact"
    val symbol: Column = col("annotation.SYMBOL") as "symbol"
    val feature_type: Column = col("annotation.Feature_type") as "feature_type"
    val ensembl_gene_id: Column = col("annotation.Gene") as "ensembl_gene_id"
    val ensembl_transcript_id: Column = when(col("annotation.Feature_type") === "Transcript", col("annotation.Feature")).otherwise(null) as "ensembl_transcript_id"
    val ensembl_regulatory_id: Column = when(col("annotation.Feature_type") === "RegulatoryFeature", col("annotation.Feature")).otherwise(null) as "ensembl_regulatory_id"
    val exon: Column = col("annotation.EXON") as "exon"
    val biotype: Column = col("annotation.BIOTYPE") as "biotype"
    val intron: Column = col("annotation.INTRON") as "intron"
    val hgvsc: Column = col("annotation.HGVSc") as "hgvsc"
    val hgvsp: Column = col("annotation.HGVSp") as "hgvsp"

    val strand: Column = col("annotation.STRAND") as "strand"
    val cds_position: Column = col("annotation.CDS_position") as "cds_position"
    val cdna_position: Column = col("annotation.cDNA_position") as "cdna_position"
    val protein_position: Column = col("annotation.Protein_position") as "protein_position"
    val amino_acids: Column = col("annotation.Amino_acids") as "amino_acids"
    val codons: Column = col("annotation.Codons") as "codons"
    val variant_class: Column = col("annotation.VARIANT_CLASS") as "variant_class"
    val hgvsg: Column = col("annotation.HGVSg") as "hgvsg"
    val original_canonical: Column = when(col("annotation.CANONICAL") === "YES", lit(true)).otherwise(lit(false)) as "original_canonical"
    val is_multi_allelic: Column = col("splitFromMultiAllelic") as "is_multi_allelic"
    val old_multi_allelic: Column = col("INFO_OLD_MULTIALLELIC") as "old_multi_allelic"

    def optional_info(df: DataFrame, colName: String, alias: String, colType: String = "string"): Column = (if (df.columns.contains(colName)) col(colName) else lit(null).cast(colType)).as(alias)

    //the order matters, do not change it
    val locusColumNames: Seq[String] = Seq("chromosome", "start", "reference", "alternate")

    val locus: Seq[Column] = locusColumNames.map(col)
  }

  val removeEmptyObjectsIn: String => Column = column => when(to_json(col(column)) === lit("[{}]"), array()).otherwise(col(column))

  def getColumnOrElse(colName: String, default: Any = ""): Column = when(col(colName).isNull, lit(default)).otherwise(trim(col(colName)))

}

