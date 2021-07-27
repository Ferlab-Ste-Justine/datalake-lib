package bio.ferlab.datalake.spark3.implicits

import io.projectglow.Glow
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, DoubleType, StringType}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}

object GenomicImplicits {

  val id: Column = sha1(concat(col("chromosome"), col("start"), col("reference"), col("alternate"))) as "id"

  implicit class GenomicOperations(df: DataFrame) {

    def joinAndMerge(other: DataFrame, outputColumnName: String, joinType: String = "inner"): DataFrame = {
      val otherFields = other.drop("chromosome", "start", "end", "name", "reference", "alternate")

      df.joinByLocus(other, joinType)
        .withColumn(outputColumnName, when(col(otherFields.columns.head).isNotNull, struct(otherFields("*"))).otherwise(lit(null)))
        .select(df.columns.map(col) :+ col(outputColumnName): _*)
    }

    def joinByLocus(other: DataFrame, joinType: String): DataFrame = {
      df.join(other, Seq("chromosome", "start", "reference", "alternate"), joinType)
    }

    def groupByLocus(): RelationalGroupedDataset = {
      df.groupBy(col("chromosome"), col("start"), col("reference"), col("alternate"))
    }

    def selectLocus(cols: Column*): DataFrame = {
      val allCols = col("chromosome") :: col("start") :: col("reference") :: col("alternate") :: cols.toList
      df.select(allCols: _*)
    }

    def withCombinedFrequencies(combined: String, prefix1: String, prefix2: String): DataFrame = {
      df
        .withColumn(s"${combined}_ac", col(s"${prefix1}_ac") + col(s"${prefix2}_ac"))
        .withColumn(s"${combined}_an", col(s"${prefix1}_an") + col(s"${prefix2}_an"))
        .withColumn(
          s"${combined}_af",
          (col(s"${combined}_ac") / col(s"${combined}_an")).cast(DecimalType(38, 18))
        )
        .withColumn(
          s"${combined}_heterozygotes",
          col(s"${prefix1}_heterozygotes") + col(s"${prefix2}_heterozygotes")
        )
        .withColumn(
          s"${combined}_homozygotes",
          col(s"${prefix1}_homozygotes") + col(s"${prefix2}_homozygotes")
        )
    }

    def withSplitMultiAllelic: DataFrame = {
      Glow.transform("split_multiallelics", df)
    }

    def withNormalizedVariants(referenceGenomePath: String = "/home/hadoop/GRCh38_full_analysis_set_plus_decoy_hla.fa"): DataFrame = {
      Glow.transform("normalize_variants", df, ("reference_genome_path", referenceGenomePath))
    }

    val normalizeCall: Column => Column = calls =>
      when(col("is_multi_allelic"), transform(calls, c => when(c === -1, lit(0)).otherwise(c))).otherwise(calls)

    val isHeterozygote: Column = col("zygosity") === "HET"

    val isSexualGenotype: Column = col("chromosome").isin("X", "Y")

    val strictAutosomalTransmissions = List(
      //(proband_calls, father_calls, mother_calls, father_affected, mother_affected, transmission)
      //(“0/1”, “0/0”, “0/0”) -> 	autosomal_dominant (de_novo) [if both parents unaffected]
      (Array(0, 1), Array(0, 0), Array(0, 0), false, false, "autosomal_dominant (de_novo)"),
      //(“0/1”, “0/0”, “0/1”) -> 	autosomal_dominant [if affected mother and unaffected father]
      (Array(0, 1), Array(0, 0), Array(0, 1), false, true , "autosomal_dominant"),
      //(“0/1”, “0/1”, “0/0”) -> 	autosomal_dominant [if affected father and unaffected mother]
      (Array(0, 1), Array(0, 1), Array(0, 0), true , false, "autosomal_dominant"),
      //(“0/1”, “0/1”, “0/1”) -> 	autosomal_dominant [if both parents affected]
      (Array(0, 1), Array(0, 1), Array(0, 1), true, true, "autosomal_dominant"),
      //(“1/1”, “0/1”, “0/1”) -> 	autosomal_recessive [if both parents unaffected]
      (Array(1, 1), Array(0, 1), Array(0, 1), false, false, "autosomal_recessive"),
      //(“1/1”, “0/1”, “1/1”) -> 	autosomal_recessive [if affected mother and unaffected father]
      (Array(1, 1), Array(0, 1), Array(1, 1), false, true, "autosomal_recessive"),
      //(“1/1”, “1/1”, “0/1”) -> 	autosomal_recessive [if affected father and unaffected mother]
      (Array(1, 1), Array(1, 1), Array(0, 1), true, false, "autosomal_recessive"),
      //(“1/1”, “1/1”, “1/1”) -> 	autosomal_recessive [if both parents affected]
      (Array(1, 1), Array(1, 1), Array(1, 1), true, true, "autosomal_recessive")
    )

    val strictSexualTransmissions = List(
      //(gender, proband_calls, father_calls, mother_calls, father_affected, mother_affected, transmission)
      //(“0/1”, “0/0”, “0/0”) -> 	x_linked_dominant (de_novo) [if female proband with both parents unaffected]
      //			                    x_linked_recessive (de_novo) [if male proband with both parents unaffected]
      ("Female", Array(0, 1), Array(0, 0), Array(0, 0), false, false, "x_linked_dominant (de_novo)"),
      ("Male"  , Array(0, 1), Array(0, 0), Array(0, 0), false, false, "x_linked_recessive (de_novo)"),
      //(“0/1”, “0/0”, “0/1”) -> 	x_linked_dominant [if female proband with affected mother and unaffected father]
      //                          x_linked_recessive [if male proband with both parents unaffected]
      ("Female", Array(0, 1), Array(0, 0), Array(0, 1), false, true , "x_linked_dominant"),
      ("Male"  , Array(0, 1), Array(0, 0), Array(0, 1), false, false, "x_linked_recessive"),
      //(“0/1”, “0/0”, “1/1”) -> 	x_linked_recessive [if male proband with affected mother and unaffected father]
      ("Male"  , Array(0, 1), Array(0, 0), Array(1, 1), false, true, "x_linked_recessive"),
      //(“0/1”, “0/1”, “0/0”) -> 	x_linked_dominant [if female proband with affected father and unaffected mother]
      ("Female", Array(0, 1), Array(0, 1), Array(0, 0), true, false, "x_linked_dominant"),
      //(“0/1”, “0/1”, “0/1”) -> 	x_linked_dominant [if female proband with both parents affected]
      //                          x_linked_recessive [if male proband with affected father and unaffected mother]
      ("Female", Array(0, 1), Array(0, 1), Array(0, 1), true, true , "x_linked_dominant"),
      ("Male"  , Array(0, 1), Array(0, 1), Array(0, 1), true, false, "x_linked_recessive"),
      //(“0/1”, “0/1”, “1/1”) ->  x_linked_recessive [if male proband with both parents affected]
      ("Male"  , Array(0, 1), Array(0, 1), Array(1, 1), true, true, "x_linked_recessive"),
      //(“0/1”, “1/1”, “0/0”) -> 	x_linked_dominant [if female proband affected father and unaffected mother]
      ("Female", Array(0, 1), Array(1, 1), Array(0, 0), true, false, "x_linked_recessive"),
      //(“0/1”, “1/1”, “0/1”) -> 	x_linked_dominant [if female proband with both parents affected]
      //                          x_linked_recessive [if male proband with affected father and unaffected mother]
      ("Female", Array(0, 1), Array(1, 1), Array(0, 1), true, true , "x_linked_dominant"),
      ("Male"  , Array(0, 1), Array(1, 1), Array(0, 1), true, false, "x_linked_recessive"),
      //(“0/1”, “1/1”, “1/1”) -> 	x_linked_recessive [if male proband with both parents affected]
      ("Male"  , Array(0, 1), Array(1, 1), Array(1, 1), true, true, "x_linked_recessive"),
      //(“1/1”, “0/0”, “0/0”) -> 	x_linked_recessive (de_novo) [if male proband with both parents unaffected]
      ("Male"  , Array(1, 1), Array(0, 0), Array(0, 0), false, false, "x_linked_recessive (de_novo)"),
      //(“1/1”, “0/0”, “0/1”) ->	x_linked_recessive [if male proband with both parents unaffected]
      ("Male"  , Array(1, 1), Array(0, 0), Array(0, 1), false, false, "x_linked_recessive"),
      //(“1/1”, “0/0”, “1/1”) -> 	x_linked_recessive [if male proband with affected mother and unaffected father]
      ("Male"  , Array(1, 1), Array(0, 0), Array(1, 1), false, true, "x_linked_recessive"),
      //(“1/1”, “0/1”, “0/1”) -> 	x_linked_recessive [if affected father and unaffected mother]
      ("Female", Array(1, 1), Array(0, 1), Array(0, 1), true, false, "x_linked_recessive"),
      ("Male"  , Array(1, 1), Array(0, 1), Array(0, 1), true, false, "x_linked_recessive"),
      //(“1/1”, “0/1”, “1/1”) -> 	x_linked_recessive [if both parents affected]
      ("Female", Array(1, 1), Array(0, 1), Array(1, 1), true, true, "x_linked_recessive"),
      ("Male"  , Array(1, 1), Array(0, 1), Array(1, 1), true, true, "x_linked_recessive"),
      //(“1/1”, “1/1”, “0/1”) -> 	x_linked_recessive [if affected father and unaffected mother]
      ("Female", Array(1, 1), Array(1, 1), Array(0, 1), true, false, "x_linked_recessive"),
      ("Male"  , Array(1, 1), Array(1, 1), Array(0, 1), true, false, "x_linked_recessive"),
      //(“1/1”, “1/1”, “1/1”) -> 	x_linked_recessive [if both parents affected]
      ("Female", Array(1, 1), Array(1, 1), Array(1, 1), true, true, "x_linked_recessive"),
      ("Male"  , Array(1, 1), Array(1, 1), Array(1, 1), true, true, "x_linked_recessive"),
    )

    def withGenotypeTransmission(as: String, fth_calls: Column, mth_calls: Column): DataFrame = {
      val normalizedCallsDf =
        df.withColumn("norm_fth_calls", normalizeCall(fth_calls))
          .withColumn("norm_mth_calls", normalizeCall(mth_calls))

      val static_transmissions =
        when(col("calls") === Array(0, 0), lit("non carrier proband"))
          .when(col("calls").isNull or col("calls") === Array(-1, -1), lit("unknown proband genotype"))

      val autosomal_transmissions: Column = strictAutosomalTransmissions.foldLeft[Column](static_transmissions){
        case (c, (proband_calls, fth_calls, mth_calls, fth_affected_status, mth_affected_status, transmission)) =>
          c.when(
            col("mother_affected_status") === mth_affected_status and
              col("father_affected_status") === fth_affected_status and
              col("calls") === proband_calls and
              col("norm_fth_calls") === fth_calls and
              col("norm_mth_calls") === mth_calls, lit(transmission))
      }

      val sexual_transmissions: Column = strictSexualTransmissions.foldLeft[Column](static_transmissions){
        case (c, (gender, proband_calls, fth_calls, mth_calls, fth_affected_status, mth_affected_status, transmission)) =>
          c.when(
            col("gender") === gender and
              col("mother_affected_status") === mth_affected_status and
              col("father_affected_status") === fth_affected_status and
              col("calls") === proband_calls and
              col("norm_fth_calls") === fth_calls and
              col("norm_mth_calls") === mth_calls, lit(transmission))
      }

      normalizedCallsDf
        .withColumn(as, when(isSexualGenotype, sexual_transmissions).otherwise(autosomal_transmissions))
        .drop("norm_fth_calls", "norm_mth_calls")
    }

    /**
     * Compute transmission per locus given an existing column containing the transmission for this particular occurrence.
     *
     * @param locusColumnNames list of locus columns
     * @param transmissionColumnName name of the transmission column
     * @param resultColumnName name of the resulting computation
     * @return a dataframe with a new column containing the number of transmission per locus as a Map of transmission type -> count per type.
     */
    def withTransmissionPerLocus(locusColumnNames: Seq[String],
                                 transmissionColumnName: String,
                                 resultColumnName: String): DataFrame = {
      df.groupBy(transmissionColumnName, locusColumnNames:_*).count()
        .groupBy(locusColumnNames.map(col):_*)
        .agg(map_from_entries(collect_list(struct(col(transmissionColumnName), col("count")))) as resultColumnName)
    }


    def withParentalOrigin(as: String, fth_calls: Column, mth_calls: Column, MTH: String = "mother", FTH: String = "father"): DataFrame = {
      val normalizedCallsDf =
        df.withColumn("norm_fth_calls", normalizeCall(fth_calls))
          .withColumn("norm_mth_calls", normalizeCall(mth_calls))

      val origins = List(
        //(father_calls, mother_calls, origin)
        (Array(0, 1), Array(0, 0), FTH),
        (Array(0, 0), Array(0, 1), MTH),
        (Array(1, 1), Array(0, 0), FTH),
        (Array(0, 0), Array(1, 1), MTH),
        (Array(1, 1), Array(0, 1), FTH),
        (Array(0, 1), Array(1, 1), MTH),
        (Array(1, 1), Array(1, 0), FTH),
        (Array(1, 0), Array(1, 1), MTH),
        (Array(1, 0), Array(0, 0), FTH),
        (Array(0, 0), Array(1, 0), MTH)
      )
      val static_origins = when(not(isHeterozygote), lit(null).cast(StringType))

      val parental_origin = origins.foldLeft[Column](static_origins){
        case (c, (fth, mth, origin)) => c.when(col("norm_fth_calls") === fth and col("norm_mth_calls") === mth, lit(origin))
      }

      normalizedCallsDf.withColumn(as, parental_origin)
        .drop("norm_fth_calls", "norm_mth_calls")
    }
  }

  object columns {
    val chromosome: Column = ltrim(col("contigName"), "chr") as "chromosome"
    val reference: Column  = col("referenceAllele") as "reference"
    val start: Column      = (col("start") + 1) as "start"
    val end: Column        = (col("end") + 1) as "end"
    val alternate: Column  = col("alternateAlleles")(0) as "alternate"
    val name: Column       = col("names")(0) as "name"

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

    val familyVariantWindow: WindowSpec =
      Window.partitionBy("chromosome", "start", "reference", "alternate", "family_id")

    val familyInfo: Column = when(col("family_id").isNotNull,
      map_from_entries(
        collect_list(
          struct(col("participant_id"), struct(col("calls"), col("affected_status")))
        ).over(familyVariantWindow)
      )
    )

    val motherCalls: Column = col("family_info")(col("mother_id"))("calls")
    val fatherCalls: Column = col("family_info")(col("father_id"))("calls")
    val motherAffectedStatus: Column = col("family_info")(col("mother_id"))("affected_status")
    val fatherAffectedStatus: Column = col("family_info")(col("father_id"))("affected_status")

    /** has_alt return 1 if there is at least one alternative allele. Note : It cannot returned a boolean beacause it's used to partition data.
     * It looks like Glue does not support partition by boolean
     */
    val has_alt: Column =
      when(array_contains(col("genotype.calls"), 1), 1).otherwise(0) as "has_alt"

    val is_normalized: Column = col("normalizationStatus.changed") as "is_normalized"

    val calculated_ac: Column = when(col("zygosity") === "HET", 1)
      .when(col("zygosity") === "HOM", 2)
      .otherwise(0) as "ac"

    val calculate_an_upper_bound_kf: Long => Column = pc => lit(pc * 2) as "an_upper_bound_kf"
    val calculate_an_lower_bound_kf: Column =
      when(col("zygosity") === "HOM" or col("zygosity") === "HET" or col("zygosity") === "WT", 2)
        .otherwise(0) as "an_lower_bound_kf"

    val homozygotes: Column   = when(col("zygosity") === "HOM", 1).otherwise(0) as "homozygotes"
    val heterozygotes: Column = when(col("zygosity") === "HET", 1).otherwise(0) as "heterozygotes"

    val zygosity: Column => Column = c =>
      when(c(0) === 1 && c(1) === 1, "HOM")
        .when(c(0) === 0 && c(1) === 1, "HET")
        .when(c(0) === 0 && c(1) === 0, "WT")
        .when(c(0) === 1 && c(1) === 0, "HET")
        .when(c.isNull, lit(null).cast("string"))
        .otherwise("UNK")

    //Annotations
    val annotations: Column = when(
      col("splitFromMultiAllelic"),
      expr("filter(INFO_ANN, ann-> ann.Allele == alternateAlleles[0])")
    ).otherwise(col("INFO_ANN")) as "annotations"
    val csq: Column = when(
      col("splitFromMultiAllelic"),
      expr("filter(INFO_CSQ, ann-> ann.Allele == alternateAlleles[0])")
    ).otherwise(col("INFO_CSQ")) as "annotations"
    val firstAnn: Column        = annotations.getItem(0) as "annotation"
    val firstCsq: Column        = csq.getItem(0) as "annotation"
    val consequences: Column    = col("annotation.Consequence") as "consequences"
    val impact: Column          = col("annotation.IMPACT") as "impact"
    val symbol: Column          = col("annotation.SYMBOL") as "symbol"
    val feature_type: Column    = col("annotation.Feature_type") as "feature_type"
    val ensembl_gene_id: Column = col("annotation.Gene") as "ensembl_gene_id"
    val ensembl_transcript_id: Column =
      when(col("annotation.Feature_type") === "Transcript", col("annotation.Feature"))
        .otherwise(null) as "ensembl_transcript_id"
    val ensembl_regulatory_id: Column =
      when(col("annotation.Feature_type") === "RegulatoryFeature", col("annotation.Feature"))
        .otherwise(null) as "ensembl_regulatory_id"
    val exon: Column    = col("annotation.EXON") as "exon"
    val biotype: Column = col("annotation.BIOTYPE") as "biotype"
    val intron: Column  = col("annotation.INTRON") as "intron"
    val hgvsc: Column   = col("annotation.HGVSc") as "hgvsc"
    val hgvsp: Column   = col("annotation.HGVSp") as "hgvsp"

    val strand: Column           = col("annotation.STRAND") as "strand"
    val cds_position: Column     = col("annotation.CDS_position") as "cds_position"
    val cdna_position: Column    = col("annotation.cDNA_position") as "cdna_position"
    val protein_position: Column = col("annotation.Protein_position") as "protein_position"
    val amino_acids: Column      = col("annotation.Amino_acids") as "amino_acids"
    val codons: Column           = col("annotation.Codons") as "codons"
    val variant_class: Column    = col("annotation.VARIANT_CLASS") as "variant_class"
    val hgvsg: Column            = col("annotation.HGVSg") as "hgvsg"
    val original_canonical: Column = when(col("annotation.CANONICAL") === "YES", lit(true))
      .otherwise(lit(false)) as "original_canonical"
    val is_multi_allelic: Column  = col("splitFromMultiAllelic") as "is_multi_allelic"
    val old_multi_allelic: Column = col("INFO_OLD_MULTIALLELIC") as "old_multi_allelic"

    def optional_info(df: DataFrame,
                      colName: String,
                      alias: String,
                      colType: String = "string"): Column =
      (if (df.columns.contains(colName)) col(colName) else lit(null).cast(colType)).as(alias)

    //the order matters, do not change it
    val locusColumNames: Seq[String] = Seq("chromosome", "start", "reference", "alternate")

    val locus: Seq[Column] = locusColumNames.map(col)
  }

  /**
   * Reads vcf files into dataframe and apply transformations:
   *  - split_multiallelics
   *  - optionally normalize_variants if a path to a reference genome is given
   *
   * @param input where the vcf files are located
   * @param referenceGenomePath reference genome path. This path has to be local for each executors
   * @param spark a Spark session
   * @return data into a dataframe
   */
  def vcf(input: String, referenceGenomePath: Option[String])(implicit spark: SparkSession): DataFrame = {
    val inputs = input.split(",")

    val df = spark.read
      .format("vcf")
      .option("flattenInfoFields", "true")
      .load(inputs: _*)
      .withColumnRenamed("filters", "INFO_FILTERS") // Avoid losing filters columns before split
      .withSplitMultiAllelic
    referenceGenomePath
      .fold(
        df.withColumn("normalizationStatus",
          struct(
            lit(false) as "changed",
            lit(null).cast(StringType) as "errorMessage"))
      )(path => df.withNormalizedVariants(path))
  }

  def vcf(files: List[String], referenceGenomePath: Option[String])(implicit spark: SparkSession): DataFrame = {
    vcf(files.mkString(","), referenceGenomePath)
  }

}

