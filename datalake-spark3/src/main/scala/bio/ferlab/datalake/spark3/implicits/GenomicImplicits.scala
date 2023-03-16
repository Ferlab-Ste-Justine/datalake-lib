package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.ParentalOrigin._
import io.projectglow.Glow
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, DoubleType, StringType}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}

import scala.collection.immutable

object GenomicImplicits {

  implicit class GenomicOperations(df: DataFrame) {

    def joinAndMerge(other: DataFrame, outputColumnName: String, joinType: String = "inner"): DataFrame = {
      val otherFields = other.drop("chromosome", "start", "end", "name", "reference", "alternate")

      df.joinByLocus(other, joinType)
        .withColumn(outputColumnName, when(col(otherFields.columns.head).isNotNull, struct(otherFields("*"))).otherwise(lit(null)))
        .select(df.columns.map(col) :+ col(outputColumnName): _*)
    }

    def joinByLocus(other: DataFrame, joinType: String): DataFrame = {
      df.join(other, columns.locusColumnNames, joinType)
    }

    def groupByLocus(): RelationalGroupedDataset = {
      df.groupBy(columns.locus: _*)
    }

    def selectLocus(cols: Column*): DataFrame = {
      val allCols = (columns.locus ++ cols).toList
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
      sort_array(
        when(col("is_multi_allelic") and array_contains(calls, -1), transform(calls, c => when(c === -1, lit(0)).otherwise(c)))
          .otherwise(calls))

    val normalizeMonosomy: Column => Column = calls =>
      when(calls === Array(1), Array(1, 1))
        .when(calls === Array(0), Array(0, 0))
        .when(calls === Array(-1), Array(-1, -1))
        .otherwise(calls)

    val isHeterozygote: Column = col("zygosity") === "HET"

    val isWildType: Column = col("zygosity") === "WT"

    val isSexualGenotype: Column = col("chromosome").isin("X", "Y")

    private val strictAutosomalTransmissions = List(
      //(proband_calls, father_calls, mother_calls, father_affected, mother_affected, transmission)
      //(“0/1”, “0/0”, “0/0”) -> 	autosomal_dominant (de_novo) [if both parents unaffected]
      (Array(0, 1), Array(0, 0), Array(0, 0), false, false, "autosomal_dominant_de_novo"),
      //(“0/1”, “0/0”, “0/1”) -> 	autosomal_dominant [if affected mother and unaffected father]
      (Array(0, 1), Array(0, 0), Array(0, 1), false, true, "autosomal_dominant"),
      //(“0/1”, “0/0”, “1/1”) -> 	autosomal_dominant [if affected mother and unaffected father]
      (Array(0, 1), Array(0, 0), Array(1, 1), false, true, "autosomal_dominant"),
      //(“0/1”, “0/1”, “0/0”) -> 	autosomal_dominant [if affected father and unaffected mother]
      (Array(0, 1), Array(0, 1), Array(0, 0), true, false, "autosomal_dominant"),
      //(“0/1”, “0/1”, “0/1”) -> 	autosomal_dominant [if both parents affected]
      (Array(0, 1), Array(0, 1), Array(1, 1), true, true, "autosomal_dominant"),
      //(“0/1”, “0/1”, “0/1”) -> 	autosomal_dominant [if both parents affected]
      (Array(0, 1), Array(0, 1), Array(0, 1), true, true, "autosomal_dominant"),
      //(“0/1”, “0/1”, “-1/-1”) -> 	autosomal_dominant [if affected father]
      (Array(0, 1), Array(0, 1), Array(-1, -1), true, true, "autosomal_dominant"),
      (Array(0, 1), Array(0, 1), Array(-1, -1), true, false, "autosomal_dominant"),
      //(“0/1”, “1/1”, “*/*”) -> 	autosomal_dominant [if affected father]
      (Array(0, 1), Array(1, 1), Array(0, 0), true, true, "autosomal_dominant"),
      (Array(0, 1), Array(1, 1), Array(0, 0), true, false, "autosomal_dominant"),
      (Array(0, 1), Array(1, 1), Array(0, 1), true, true, "autosomal_dominant"),
      (Array(0, 1), Array(1, 1), Array(0, 1), true, false, "autosomal_dominant"),
      (Array(0, 1), Array(1, 1), Array(1, 1), true, true, "autosomal_dominant"),
      (Array(0, 1), Array(1, 1), Array(1, 1), true, false, "autosomal_dominant"),
      (Array(0, 1), Array(1, 1), Array(-1, -1), true, true, "autosomal_dominant"),
      (Array(0, 1), Array(1, 1), Array(-1, -1), true, false, "autosomal_dominant"),
      //(“0/1”, “-1/-1”, “0/1”) -> 	autosomal_dominant [if affected mother]
      (Array(0, 1), Array(-1, -1), Array(0, 1), true, true, "autosomal_dominant"),
      (Array(0, 1), Array(-1, -1), Array(0, 1), false, true, "autosomal_dominant"),
      //(“0/1”, “-1/-1”, “1/1”) -> 	autosomal_dominant [if affected mother]
      (Array(0, 1), Array(-1, -1), Array(1, 1), true, true, "autosomal_dominant"),
      (Array(0, 1), Array(-1, -1), Array(1, 1), false, true, "autosomal_dominant"),
      //(“1/1”, “0/1”, “0/1”) -> 	autosomal_recessive [if both parents unaffected]
      (Array(1, 1), Array(0, 1), Array(0, 1), false, false, "autosomal_recessive"),
      //(“1/1”, “0/1”, “1/1”) -> 	autosomal_recessive [if affected mother and unaffected father]
      (Array(1, 1), Array(0, 1), Array(1, 1), false, true, "autosomal_recessive"),
      //(“1/1”, “1/1”, “0/1”) -> 	autosomal_recessive [if affected father and unaffected mother]
      (Array(1, 1), Array(1, 1), Array(0, 1), true, false, "autosomal_recessive"),
      //(“1/1”, “1/1”, “1/1”) -> 	autosomal_recessive [if both parents affected]
      (Array(1, 1), Array(1, 1), Array(1, 1), true, true, "autosomal_recessive"),
    )

    private val strictSexualTransmissions = List(
      //(gender, proband_calls, father_calls, mother_calls, father_affected, mother_affected, transmission)
      //(“0/1”, “0”, “0/0”) -> 	x_linked_dominant (de_novo) [if female proband with both parents unaffected]
      ("Female", Array(0, 1), Array(0, 0), Array(0, 0), false, false, "x_linked_dominant_de_novo"),
      //(“1”, “0”, “0/0”) -> 	x_linked_recessive (de_novo) [if male proband with both parents unaffected]
      ("Male", Array(1, 1), Array(0, 0), Array(0, 0), false, false, "x_linked_recessive_de_novo"),
      //(“0/1”, “0”, “0/1”) -> 	x_linked_dominant [if female proband with affected mother and unaffected father]
      ("Female", Array(0, 1), Array(0, 0), Array(0, 1), false, true, "x_linked_dominant"),
      //(“1”, “0”, “0/1”) -> 	x_linked_recessive [if male proband with both parents unaffected]
      ("Male", Array(1, 1), Array(0, 0), Array(0, 1), false, false, "x_linked_recessive"),
      //(“1”, “0”, “1/1”) -> 	x_linked_recessive [if male proband with affected mother and unaffected father]
      ("Male", Array(1, 1), Array(0, 0), Array(1, 1), false, true, "x_linked_recessive"),
      //(“0/1”, “1”, “0/0”)   -> x_linked_dominant [if female proband with affected father and unaffected mother]
      ("Female", Array(0, 1), Array(1, 1), Array(0, 0), true, false, "x_linked_dominant"),
      //(“1”, “1”, “0/0”)     -> x_linked_recessive [if male proband with affected father and unaffected mother]
      ("Male", Array(1, 1), Array(1, 1), Array(0, 0), true, false, "x_linked_recessive"),
      //(“0/1”, “1”, “0/1”)   -> x_linked_dominant [if female proband with both parents affected]
      ("Female", Array(0, 1), Array(1, 1), Array(0, 1), true, true, "x_linked_dominant"),
      //(“1”, “1”, “0/1”)     -> x_linked_recessive [if male proband with affected father and unaffected mother]
      ("Male", Array(1, 1), Array(1, 1), Array(0, 1), true, false, "x_linked_recessive"),
      //(“0/1”, “1”, “1/1”) -> 	x_linked_recessive [if male proband with both parents affected]
      ("Male", Array(0, 1), Array(1, 1), Array(0, 1), true, true, "x_linked_recessive"),
      ("Male", Array(1, 1), Array(1, 1), Array(1, 1), true, true, "x_linked_recessive"),
      //(“0/1”, “./.”, “0/1”) -> x_linked_dominant [if female proband with affected mother]
      ("Female", Array(0, 1), Array(-1, -1), Array(0, 1), false, true, "x_linked_dominant"),
      ("Female", Array(0, 1), Array(-1, -1), Array(0, 1), true, true, "x_linked_dominant"),
      //(“1”, “./.”, “0/1”)   -> x_linked_recessive [if male proband with unaffected mother]
      ("Male", Array(1, 1), Array(-1, -1), Array(0, 1), true, false, "x_linked_recessive"),
      ("Male", Array(1, 1), Array(-1, -1), Array(0, 1), false, false, "x_linked_recessive"),
      //(“1”, “./.”, “1/1”)   -> x_linked_recessive [if male proband with affected mother]
      ("Male", Array(1, 1), Array(-1, -1), Array(1, 1), false, true, "x_linked_recessive"),
      ("Male", Array(1, 1), Array(-1, -1), Array(1, 1), true, true, "x_linked_recessive"),
      //(“1/1”, “1”, “0/0”)   -> x_linked_recessive [if female proband with affected father and unaffected mother]
      ("Female", Array(1, 1), Array(1, 1), Array(0, 0), true, false, "x_linked_recessive"),
      //(“1/1”, “1”, “0/1”)   -> x_linked_recessive [if female proband with affected father and unaffected mother]
      ("Female", Array(1, 1), Array(1, 1), Array(0, 1), true, false, "x_linked_recessive"),
      //(“1/1”, “1”, “1/1”)   -> x_linked_recessive [if female proband with both parents affected]
      ("Female", Array(1, 1), Array(1, 1), Array(1, 1), true, true, "x_linked_recessive"),
      //(“0/1”, “1”, “./.”) -> 	x_linked_dominant [if female proband with affected father]
      ("Female", Array(1, 1), Array(1, 1), Array(-1, -1), true, true, "x_linked_dominant"),
      ("Female", Array(1, 1), Array(1, 1), Array(-1, -1), true, false, "x_linked_dominant"),
    )

    def withGenotypeTransmission(as: String,
                                 calls_name: String = "calls",
                                 gender_name: String = "gender",
                                 affected_status_name: String = "affected_status",
                                 father_calls_name: String = "father_calls",
                                 father_affected_status_name: String = "father_affected_status",
                                 mother_calls_name: String = "mother_calls",
                                 mother_affected_status_name: String = "mother_affected_status"): DataFrame = {
      val calls = col(calls_name)
      val fth_calls = col(father_calls_name)
      val mth_calls = col(mother_calls_name)

      val normalizedCallsDf =
        df.withColumn("norm_calls", normalizeCall(normalizeMonosomy(calls)))
          .withColumn("norm_fth_calls", normalizeCall(normalizeMonosomy(fth_calls)))
          .withColumn("norm_mth_calls", normalizeCall(normalizeMonosomy(mth_calls)))

      val static_transmissions = {
        when(col("norm_fth_calls").isNull or col("norm_mth_calls").isNull, lit(null))
          .when(col("norm_calls") === Array(0, 0), lit("non_carrier_proband"))
          .when(col("norm_calls").isNull or col("norm_calls") === Array(-1, -1), lit("unknown_proband_genotype"))
      }

      val autosomal_transmissions: Column = strictAutosomalTransmissions.foldLeft[Column](static_transmissions) {
        case (c, (proband_calls, fth_calls, mth_calls, fth_affected_status, mth_affected_status, transmission)) =>
          c.when(
            col(mother_affected_status_name) === mth_affected_status and
              col(father_affected_status_name) === fth_affected_status and
              col("norm_calls") === proband_calls and
              col("norm_fth_calls") === fth_calls and
              col("norm_mth_calls") === mth_calls, lit(transmission))
      }

      val sexual_transmissions: Column = strictSexualTransmissions.foldLeft[Column](static_transmissions) {
        case (c, (gender, proband_calls, fth_calls, mth_calls, fth_affected_status, mth_affected_status, transmission)) =>
          c.when(
            col(gender_name) === gender and
              col(mother_affected_status_name) === mth_affected_status and
              col(father_affected_status_name) === fth_affected_status and
              col("norm_calls") === proband_calls and
              col("norm_fth_calls") === fth_calls and
              col("norm_mth_calls") === mth_calls, lit(transmission))
      }

      normalizedCallsDf
        .withColumn(as, when(isSexualGenotype, sexual_transmissions).otherwise(autosomal_transmissions))
        .withColumn(as,
          when(col(as).isNull and fth_calls.isNull and mth_calls.isNull, lit("unknown_parents_genotype"))
            .when(col(as).isNull and fth_calls.isNull, lit("unknown_father_genotype"))
            .when(col(as).isNull and mth_calls.isNull, lit("unknown_mother_genotype")).otherwise(col(as)))
        .drop("norm_calls", "norm_fth_calls", "norm_mth_calls")
    }

    /**
     * Compute transmission per locus given an existing column containing the transmission for this particular occurrence.
     *
     * @param locusColumnNames       list of locus columns
     * @param transmissionColumnName name of the transmission column
     * @param resultColumnName       name of the resulting computation
     * @return a dataframe with a new column containing the number of transmission per locus as a Map of transmission type -> count per type.
     */
    def withTransmissionPerLocus(locusColumnNames: Seq[String],
                                 transmissionColumnName: String,
                                 resultColumnName: String): DataFrame = {
      df.groupBy(transmissionColumnName, locusColumnNames: _*).count()
        .groupBy(locusColumnNames.map(col): _*)
        .agg(map_from_entries(collect_list(struct(col(transmissionColumnName), col("count")))) as resultColumnName)
    }


    def withParentalOrigin(as: String, calls: Column, fth_calls: Column, mth_calls: Column): DataFrame = {

      val normalized: Column => Column = c => sort_array(when(c.isNull, array(lit(-1), lit(-1))).otherwise(c))

      val normalizedCallsDf =
        df.withColumn("norm_calls", normalized(calls))
          .withColumn("norm_fth_calls", normalized(fth_calls))
          .withColumn("norm_mth_calls", normalized(mth_calls))

      val autosomalOriginsMatrix = List(
        //(proband_calls, father_calls, mother_calls, origin)
        (Array(0, 1), Array(0, 0), Array(0, 0), DENOVO),
        (Array(0, 1), Array(0, 0), Array(0, 1), MTH),
        (Array(0, 1), Array(0, 0), Array(1, 1), MTH),
        (Array(0, 1), Array(0, 0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(0, 1), Array(0, 1), Array(0, 0), FTH),
        (Array(0, 1), Array(0, 1), Array(0, 1), AMBIGUOUS),
        (Array(0, 1), Array(0, 1), Array(1, 1), MTH),
        (Array(0, 1), Array(0, 1), Array(-1, -1), POSSIBLE_FATHER),
        (Array(0, 1), Array(1, 1), Array(0, 0), FTH),
        (Array(0, 1), Array(1, 1), Array(0, 1), FTH),
        (Array(0, 1), Array(1, 1), Array(1, 1), AMBIGUOUS),
        (Array(0, 1), Array(1, 1), Array(-1, -1), FTH),
        (Array(0, 1), Array(-1, -1), Array(0, 0), POSSIBLE_DENOVO),
        (Array(0, 1), Array(-1, -1), Array(0, 1), POSSIBLE_MOTHER),
        (Array(0, 1), Array(-1, -1), Array(1, 1), MTH),
        (Array(0, 1), Array(-1, -1), Array(-1, -1), UNKNOWN),
        (Array(1, 1), Array(0, 0), Array(0, 0), DENOVO),
        (Array(1, 1), Array(0, 0), Array(0, 1), MTH),
        (Array(1, 1), Array(0, 0), Array(1, 1), MTH),
        (Array(1, 1), Array(0, 0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(1, 1), Array(0, 1), Array(0, 0), FTH),
        (Array(1, 1), Array(0, 1), Array(0, 1), BOTH),
        (Array(1, 1), Array(0, 1), Array(1, 1), BOTH),
        (Array(1, 1), Array(0, 1), Array(-1, -1), FTH),
        (Array(1, 1), Array(1, 1), Array(0, 0), FTH),
        (Array(1, 1), Array(1, 1), Array(0, 1), BOTH),
        (Array(1, 1), Array(1, 1), Array(1, 1), BOTH),
        (Array(1, 1), Array(1, 1), Array(-1, -1), FTH),
        (Array(1, 1), Array(-1, -1), Array(0, 0), POSSIBLE_DENOVO),
        (Array(1, 1), Array(-1, -1), Array(0, 1), MTH),
        (Array(1, 1), Array(-1, -1), Array(1, 1), MTH),
        (Array(1, 1), Array(-1, -1), Array(-1, -1), UNKNOWN)
      )

      val XOriginsMatrix: immutable.Seq[(Array[Int], Array[Int], Array[Int], String)] = List(
        //(proband_calls, father_calls, mother_calls, origin)
        (Array(0, 1), Array(0), Array(0, 0), DENOVO),
        (Array(1), Array(0), Array(0, 0), DENOVO),
        (Array(1, 1), Array(0), Array(0, 0), DENOVO),
        (Array(0, 1), Array(0), Array(0, 1), MTH),
        (Array(1), Array(0), Array(0, 1), MTH),
        (Array(1, 1), Array(0), Array(0, 1), MTH),
        (Array(0, 1), Array(0), Array(1, 1), MTH),
        (Array(1), Array(0), Array(1, 1), MTH),
        (Array(1, 1), Array(0), Array(1, 1), MTH),
        (Array(0, 1), Array(1), Array(0, 0), FTH),
        (Array(1), Array(1), Array(0, 0), FTH),
        (Array(1, 1), Array(1), Array(0, 0), FTH),
        (Array(0, 1), Array(1), Array(0, 1), FTH),
        (Array(1), Array(1), Array(0, 1), MTH),
        (Array(1, 1), Array(1), Array(0, 1), BOTH),
        (Array(0, 1), Array(1), Array(1, 1), AMBIGUOUS),
        (Array(1), Array(1), Array(1, 1), MTH),
        (Array(1, 1), Array(1), Array(1, 1), BOTH),
        (Array(0, 1), Array(0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(0, 1), Array(1), Array(-1, -1), FTH),
        (Array(0, 1), Array(-1), Array(0, 0), POSSIBLE_DENOVO),
        (Array(0, 1), Array(-1), Array(0, 1), POSSIBLE_MOTHER),
        (Array(0, 1), Array(-1), Array(1, 1), MTH),
        (Array(0, 1), Array(-1), Array(-1, -1), UNKNOWN),
        (Array(0, 1), Array(-1, -1), Array(-1, -1), UNKNOWN),
        (Array(1, 1), Array(0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(1, 1), Array(1), Array(-1, -1), FTH),
        (Array(1, 1), Array(-1), Array(0, 0), POSSIBLE_DENOVO),
        (Array(1, 1), Array(-1), Array(0, 1), MTH),
        (Array(1, 1), Array(-1), Array(1, 1), MTH),
        (Array(1, 1), Array(-1), Array(-1, -1), UNKNOWN),
        (Array(1, 1), Array(-1, -1), Array(-1, -1), UNKNOWN),
        (Array(1), Array(0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(1), Array(1), Array(-1, -1), POSSIBLE_FATHER),
        (Array(1), Array(-1), Array(0, 0), POSSIBLE_DENOVO),
        (Array(1), Array(-1), Array(0, 1), MTH),
        (Array(1), Array(-1), Array(1, 1), MTH),
        (Array(1), Array(-1), Array(-1, -1), UNKNOWN),
        (Array(1), Array(-1, -1), Array(-1, -1), UNKNOWN),
        (Array(0, 1), Array(0, 0), Array(0, 0), DENOVO),
        (Array(1), Array(0, 0), Array(0, 0), DENOVO),
        (Array(1, 1), Array(0, 0), Array(0, 0), DENOVO),
        (Array(0, 1), Array(0, 0), Array(0, 1), MTH),
        (Array(1), Array(0, 0), Array(0, 1), MTH),
        (Array(1, 1), Array(0, 0), Array(0, 1), MTH),
        (Array(0, 1), Array(0, 0), Array(1, 1), MTH),
        (Array(1), Array(0, 0), Array(1, 1), MTH),
        (Array(1, 1), Array(0, 0), Array(1, 1), MTH),
        (Array(0, 1), Array(0, 1), Array(0, 0), FTH),
        (Array(0, 1), Array(1, 1), Array(0, 0), FTH),
        (Array(1), Array(0, 1), Array(0, 0), FTH),
        (Array(1), Array(1, 1), Array(0, 0), FTH),
        (Array(1, 1), Array(0, 1), Array(0, 0), FTH),
        (Array(1, 1), Array(1, 1), Array(0, 0), FTH),
        (Array(0, 1), Array(0, 1), Array(0, 1), AMBIGUOUS),
        (Array(0, 1), Array(1, 1), Array(0, 1), FTH),
        (Array(1), Array(0, 1), Array(0, 1), AMBIGUOUS),
        (Array(1), Array(1, 1), Array(0, 1), AMBIGUOUS),
        (Array(1, 1), Array(0, 1), Array(0, 1), BOTH),
        (Array(1, 1), Array(1, 1), Array(0, 1), BOTH),
        (Array(0, 1), Array(0, 1), Array(1, 1), AMBIGUOUS),
        (Array(0, 1), Array(1, 1), Array(1, 1), AMBIGUOUS),
        (Array(1), Array(0, 1), Array(1, 1), MTH),
        (Array(1), Array(1, 1), Array(1, 1), MTH),
        (Array(1, 1), Array(0, 1), Array(1, 1), BOTH),
        (Array(1, 1), Array(1, 1), Array(1, 1), BOTH),
        (Array(0, 1), Array(0, 0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(0, 1), Array(0, 1), Array(-1, -1), FTH),
        (Array(0, 1), Array(1, 1), Array(-1, -1), FTH),
        (Array(1, 1), Array(0, 0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(1, 1), Array(0, 1), Array(-1, -1), FTH),
        (Array(1, 1), Array(1, 1), Array(-1, -1), FTH),
        (Array(1), Array(0, 0), Array(-1, -1), POSSIBLE_DENOVO),
        (Array(1), Array(0, 1), Array(-1, -1), POSSIBLE_FATHER),
        (Array(1), Array(1, 1), Array(-1, -1), POSSIBLE_FATHER)
      )

      val YOriginsMatrix = List(
        //(proband_calls, father_calls, mother_calls, origin)
        (Array(1), Array(1), Array(-1, -1), FTH),
        (Array(1), Array(0), Array(-1, -1), DENOVO),
        (Array(1, 1), Array(1), Array(-1, -1), FTH),
        (Array(1), Array(1, 1), Array(-1, -1), FTH),
        (Array(1, 1), Array(1, 1), Array(-1, -1), FTH),
        (Array(1, 1), Array(0), Array(-1, -1), DENOVO),
        (Array(1), Array(0, 0), Array(-1, -1), DENOVO),
        (Array(1, 1), Array(0, 0), Array(-1, -1), DENOVO),
        (Array(1), Array(-1, -1), Array(-1, -1), UNKNOWN),
        (Array(1, 1), Array(-1, -1), Array(-1, -1), UNKNOWN),
        (Array(1), Array(1), Array(0, 0), FTH),
        (Array(1), Array(1), Array(0, 1), AMBIGUOUS),
        (Array(1), Array(1, 1), Array(1, 1), AMBIGUOUS),
        (Array(1), Array(1, 1), Array(0, 0), FTH),
        (Array(1), Array(1, 1), Array(0, 1), AMBIGUOUS),
        (Array(1), Array(0, 1), Array(0, 0), FTH),
        (Array(1), Array(0, 1), Array(0, 1), AMBIGUOUS),
        (Array(1), Array(0, 1), Array(1, 1), AMBIGUOUS),
        (Array(1), Array(0), Array(0, 0), DENOVO),
        (Array(1), Array(0), Array(0, 1), MTH),
        (Array(1), Array(0, 0), Array(0, 0), DENOVO),
        (Array(1), Array(0, 0), Array(0, 1), MTH),
        (Array(1, 1), Array(1), Array(0, 0), FTH),
        (Array(1, 1), Array(1), Array(0, 1), BOTH),
        (Array(1, 1), Array(1), Array(1, 1), BOTH),
        (Array(1, 1), Array(1, 1), Array(0, 0), FTH),
        (Array(1, 1), Array(1, 1), Array(0, 1), BOTH),
        (Array(1, 1), Array(1, 1), Array(1, 1), BOTH),
        (Array(1, 1), Array(0, 1), Array(0, 0), FTH),
        (Array(1, 1), Array(0, 1), Array(0, 1), BOTH),
        (Array(1, 1), Array(0, 1), Array(1, 1), BOTH),
        (Array(1, 1), Array(0), Array(0, 0), DENOVO),
        (Array(1, 1), Array(0), Array(0, 1), MTH),
        (Array(1, 1), Array(0), Array(1, 1), MTH),
        (Array(1, 1), Array(0, 0), Array(0, 0), DENOVO),
        (Array(1, 1), Array(0, 0), Array(0, 1), MTH),
        (Array(1, 1), Array(0, 0), Array(1, 1), MTH),
        (Array(1), Array(0, 0), Array(1, 1), MTH)
      )
      val static_origins = when(isWildType, lit(null).cast(StringType))

      def matchOrigin(matrix: Seq[(Array[Int], Array[Int], Array[Int], String)]) = matrix.foldLeft[Column](static_origins) {
        case (c, (proband, fth, mth, origin)) => c.when(col("norm_calls") === proband and col("norm_fth_calls") === fth and col("norm_mth_calls") === mth, lit(origin))
      }.otherwise(lit(null).cast(StringType))

      val autosomalParentalOrigin = matchOrigin(autosomalOriginsMatrix)
      val XParentalOrigin: Column = matchOrigin(XOriginsMatrix)
      val YParentalOrigin: Column = matchOrigin(YOriginsMatrix)

      normalizedCallsDf
        .withColumn(as,
          when(col("chromosome") === "X", XParentalOrigin)
            .when(col("chromosome") === "Y", YParentalOrigin)
            .otherwise(autosomalParentalOrigin))
        .drop("norm_calls", "norm_fth_calls", "norm_mth_calls")
    }

    def withCompoundHeterozygous(patientIdColumnName: String = "patient_id", geneSymbolsColumnName: String = "symbols", additionalFilter: Option[Column] = None): DataFrame = {
      val filters: Column = additionalFilter.map(f => isHeterozygote and f).getOrElse(isHeterozygote)
      val het = df.filter(filters)
      val hc: DataFrame = het.getCompoundHet(patientIdColumnName, geneSymbolsColumnName)
      val possiblyHC: DataFrame = het.getPossiblyCompoundHet(patientIdColumnName, geneSymbolsColumnName)
      df
        .join(hc, Seq("chromosome", "start", "reference", "alternate", patientIdColumnName), "left")
        .join(possiblyHC, Seq("chromosome", "start", "reference", "alternate", patientIdColumnName), "left")
        .withColumn("is_hc", coalesce(col("is_hc"), lit(false)))
        .withColumn("hc_complement", coalesce(col("hc_complement"), array()))
        .withColumn("is_possibly_hc", coalesce(col("is_possibly_hc"), lit(false)))
        .withColumn("possibly_hc_complement", coalesce(col("possibly_hc_complement"), array()))
    }

    def getPossiblyCompoundHet(patientIdColumnName: String, geneSymbolsColumnName: String): DataFrame = {
      val hcWindow = Window.partitionBy(patientIdColumnName, "chromosome", "symbol").orderBy("start").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      val possiblyHC = df
        .select(col(patientIdColumnName), col("chromosome"), col("start"), col("reference"), col("alternate"), explode(col(geneSymbolsColumnName)) as "symbol")
        .withColumn("possibly_hc_count", count(lit(1)).over(hcWindow))
        .filter(col("possibly_hc_count") > 1)
        .withColumn("possibly_hc_complement", struct(col("symbol") as "symbol", col("possibly_hc_count") as "count"))
        .groupBy(col(patientIdColumnName) :: columns.locus: _*)
        .agg(collect_set("possibly_hc_complement") as "possibly_hc_complement")
        .withColumn("is_possibly_hc", lit(true))
      possiblyHC
    }

    def getCompoundHet(patientIdColumnName: String, geneSymbolsColumnName: String): DataFrame = {
      val withParentalOrigin = df.filter(col("parental_origin").isin(FTH, MTH))

      val hcWindow = Window.partitionBy(patientIdColumnName, "chromosome", "symbol", "parental_origin").orderBy("start")
      val hc = withParentalOrigin
        .select(col(patientIdColumnName), col("chromosome"), col("start"), col("reference"), col("alternate"), col(geneSymbolsColumnName), col("parental_origin"))
        .withColumn("locus", concat_ws("-", columns.locus: _*))
        .withColumn("symbol", explode(col(geneSymbolsColumnName)))
        .withColumn("coords", collect_set(col("locus")).over(hcWindow))
        .withColumn("merged_coords", last("coords", ignoreNulls = true).over(hcWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        .withColumn("struct_coords", struct(col("parental_origin"), col("merged_coords").alias("coords")))
        .withColumn("all_coords", collect_set("struct_coords").over(Window.partitionBy(patientIdColumnName, "chromosome", "symbol").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        .withColumn("complement_coords", filter(col("all_coords"), x => x.getItem("parental_origin") =!= col("parental_origin"))(0))
        .withColumn("is_hc", col("complement_coords").isNotNull)
        .filter(col("is_hc"))
        .withColumn("hc_complement", struct(col("symbol") as "symbol", col("complement_coords.coords") as "locus"))
        .groupBy(col(patientIdColumnName) :: columns.locus: _*)
        .agg(first("is_hc") as "is_hc", collect_set("hc_complement") as "hc_complement")
      hc
    }

    def pickRandomCsqPerLocus(transcriptIdColumnName: String = "ensembl_transcript_id"): DataFrame = {
      df
        .groupByLocus()
        .agg(first(transcriptIdColumnName) as transcriptIdColumnName)
    }

    /**
     * Pick a consequence with maximum impact for each locus, according to prioritization algorithm.
     */
    def withPickedCsqPerLocus(genes: DataFrame,
                              impactScoreColumnName: String = "impact_score",
                              transcriptIdColumnName: String = "ensembl_transcript_id",
                              geneSymbolColumnName: String = "symbol",
                              omimGeneIdColumnName: String = "omim_gene_id",
                              biotypeColumnName: String = "biotype",
                              maneSelectColumnName: String = "mane_select",
                              canonicalColumnName: String = "canonical",
                              manePlusColumnName: String = "mane_plus",
                              pickedColumnName: String = "picked"): DataFrame = {
      val locusWindow = Window.partitionBy(columns.locus: _*)

      // Max impact_score consequence by variant
      val maxImpactScores = df.withColumn("max_impact_score", max(impactScoreColumnName).over(locusWindow))

      // Consequences where impact_score is max_impact_score
      val maxImpactCsq: DataFrame = maxImpactScores
        .where(col("max_impact_score") === col(impactScoreColumnName))

      // Pick variants where only one consequence is max_impact_score
      val picked1MaxImpactCsq = maxImpactCsq
        .withColumn("nb_max_impact_by_var", count("*").over(locusWindow))
        .where(col("nb_max_impact_by_var") === 1)
        .selectLocus(col(transcriptIdColumnName))

      // Else, if multiple consequences are max_impact_score, join with OMIM
      val joinWithOmim = maxImpactCsq
        .joinByLocus(broadcast(picked1MaxImpactCsq), "left_anti") // remove already picked variants
        .join(genes.select(geneSymbolColumnName, omimGeneIdColumnName), Seq(geneSymbolColumnName), "left")

      // Check if at least one csq in OMIM genes
      val inOmimCsq = joinWithOmim.where(col(omimGeneIdColumnName).isNotNull)
      val noOmimCsq = joinWithOmim.joinByLocus(inOmimCsq, "left_anti")

      // For non-OMIM consequences, check if at least one csq is protein coding
      // If no OMIM csq and no protein coding csq, pick random csq for each variant
      val proteinCodingCsq = noOmimCsq.where(col(biotypeColumnName) === "protein_coding")
      val pickedNoProteinCodingCsq = noOmimCsq
        .joinByLocus(broadcast(proteinCodingCsq), "left_anti")
        .pickRandomCsqPerLocus()

      // If in OMIM csq or protein coding csq, check if at least one csq is mane select
      // If at least one csq is mane_select, pick random csq for each variant
      val pickedManeSelectCsq = inOmimCsq.unionByName(proteinCodingCsq)
        .where(col(maneSelectColumnName))
        .pickRandomCsqPerLocus()
      val noManeSelectCsq = inOmimCsq.unionByName(proteinCodingCsq)
        .joinByLocus(pickedManeSelectCsq, "left_anti")

      // If no mane select csq, check if at least one csq is canonical
      // If at least one csq is canonical, pick a random csq for each variant
      val pickedCanonicalCsq = noManeSelectCsq
        .where(col(canonicalColumnName))
        .pickRandomCsqPerLocus()
      val noCanonicalCsq = noManeSelectCsq.joinByLocus(broadcast(pickedCanonicalCsq), "left_anti")

      // If no canonical csq, check if at least one csq is mane plus
      // If at least one csq is mane plus, pick random csq for each variant
      // Else, pick random csq
      val pickedManePlusCsq = noCanonicalCsq
        .where(col(manePlusColumnName))
        .pickRandomCsqPerLocus()
      val pickedNoManePlusCsq = noCanonicalCsq
        .joinByLocus(broadcast(pickedManePlusCsq), "left_anti")
        .pickRandomCsqPerLocus()

      // Union all picked consequences and set flag to true
      val pickedCsq = picked1MaxImpactCsq
        .unionByName(pickedNoProteinCodingCsq)
        .unionByName(pickedManeSelectCsq)
        .unionByName(pickedCanonicalCsq)
        .unionByName(pickedManePlusCsq)
        .unionByName(pickedNoManePlusCsq)
        .withColumn(pickedColumnName, lit(true))
        .selectLocus(col(transcriptIdColumnName), col(pickedColumnName))

      // Join with all consequences to add picked flag
      val joinExpr: Column = columns.locusColumnNames.map(c => df(c) === pickedCsq(c)).reduce((c1, c2) => c1 && c2) &&
        df(transcriptIdColumnName) <=> pickedCsq(transcriptIdColumnName) // column can be null

      df.as("csq")
        .join(pickedCsq.as("pickedCsq"), joinExpr, "left")
        .select("csq.*", "pickedCsq.picked")
    }
  }

  object ParentalOrigin {
    val MTH: String = "mother"
    val FTH: String = "father"
    val DENOVO: String = "denovo"
    val POSSIBLE_DENOVO: String = "possible_denovo"
    val POSSIBLE_FATHER: String = "possible_father"
    val POSSIBLE_MOTHER: String = "possible_mother"
    val BOTH: String = "both"
    val AMBIGUOUS: String = "ambiguous"
    val UNKNOWN: String = "unknown"
  }

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

    def flattenInfo(df: DataFrame, except: String*): Seq[Column] =
      df.columns.filterNot(except.contains(_)).collect {
        case c if c.startsWith("INFO_") => col(c)(0) as c.replace("INFO_", "").toLowerCase
      }

    val familyVariantWindow: WindowSpec =
      Window.partitionBy("chromosome", "start", "reference", "alternate", "family_id")

    def familyInfo(cols: Seq[Column] = Seq(col("calls"), col("affected_status"), col("gq"))): Column =
      when(col("family_id").isNotNull,
        map_from_entries(
          collect_list(
            struct(col("participant_id"), struct(cols: _*))
          ).over(familyVariantWindow)
        )
      )

    val motherCalls: Column = col("family_info")(col("mother_id"))("calls")
    val motherAffectedStatus: Column = col("family_info")(col("mother_id"))("affected_status")
    val motherGQ: Column = col("family_info")(col("mother_id"))("gq")
    val motherDP: Column = col("family_info")(col("mother_id"))("dp")
    val motherQD: Column = col("family_info")(col("mother_id"))("qd")
    val motherFilters: Column = col("family_info")(col("mother_id"))("filters")
    val motherADRef: Column = col("family_info")(col("mother_id"))("ad_ref")
    val motherADAlt: Column = col("family_info")(col("mother_id"))("ad_alt")
    val motherADTotal: Column = col("family_info")(col("mother_id"))("ad_total")
    val motherADRatio: Column = col("family_info")(col("mother_id"))("ad_ratio")

    val fatherCalls: Column = col("family_info")(col("father_id"))("calls")
    val fatherAffectedStatus: Column = col("family_info")(col("father_id"))("affected_status")
    val fatherGQ: Column = col("family_info")(col("father_id"))("gq")
    val fatherDP: Column = col("family_info")(col("father_id"))("dp")
    val fatherQD: Column = col("family_info")(col("father_id"))("qd")
    val fatherFilters: Column = col("family_info")(col("father_id"))("filters")
    val fatherADRef: Column = col("family_info")(col("father_id"))("ad_ref")
    val fatherADAlt: Column = col("family_info")(col("father_id"))("ad_alt")
    val fatherADTotal: Column = col("family_info")(col("father_id"))("ad_total")
    val fatherADRatio: Column = col("family_info")(col("father_id"))("ad_ratio")

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

    val homozygotes: Column = when(col("zygosity") === "HOM", 1).otherwise(0) as "homozygotes"
    val heterozygotes: Column = when(col("zygosity") === "HET", 1).otherwise(0) as "heterozygotes"

    val zygosity: Column => Column = c =>
      when(c(0) === 1 && c(1) === 1, "HOM")
        .when(c(0) === 0 && c(1) === 1, "HET")
        .when(c(0) === 1 && c(1) === 0, "HET")
        .when(c(0) === 1 && c(1) === -1, "HET")
        .when(c(0) === -1 && c(1) === 1, "HET")
        .when(c === lit(Array(1)), "HEM")
        .when(c === lit(Array(0)), "WT")
        .when(c(0) === 0 && c(1) === 0, "WT")
        .when(c(1) === -1 && c(1) === -1, "WT")
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

    val firstAnn: Column = annotations.getItem(0) as "annotation"
    val firstCsq: Column = csq.getItem(0) as "annotation"
    val consequences: Column = col("annotation.Consequence") as "consequences"
    val impact: Column = col("annotation.IMPACT") as "impact"
    val symbol: Column = col("annotation.SYMBOL") as "symbol"
    val feature_type: Column = col("annotation.Feature_type") as "feature_type"
    val ensembl_gene_id: Column = col("annotation.Gene") as "ensembl_gene_id"
    val ensembl_feature_id: Column = col("annotation.Feature") as "ensembl_feature_id"
    val pubmed: Column = split(col("annotation.PUBMED"), "&") as "pubmed"
    val pick: Column = when(col("annotation.PICK") === "1", lit(true)).otherwise(false) as "pick"
    val ensembl_transcript_id: Column =
      when(col("annotation.Feature_type") === "Transcript", col("annotation.Feature"))
        .otherwise(null) as "ensembl_transcript_id"
    val ensembl_regulatory_id: Column =
      when(col("annotation.Feature_type") === "RegulatoryFeature", col("annotation.Feature"))
        .otherwise(null) as "ensembl_regulatory_id"
    val exon: Column = col("annotation.EXON") as "exon"
    val biotype: Column = col("annotation.BIOTYPE") as "biotype"
    val intron: Column = col("annotation.INTRON") as "intron"
    val hgvsc: Column = col("annotation.HGVSc") as "hgvsc"
    val hgvsp: Column = col("annotation.HGVSp") as "hgvsp"
    val formatted_consequence: Column => Column = c => regexp_replace(regexp_replace(c, "_variant", ""), "_", " ")
    val formatted_consequences: Column = transform(col("consequences"), formatted_consequence)
    val strand: Column = col("annotation.STRAND") as "strand"
    val cds_position: Column = col("annotation.CDS_position") as "cds_position"
    val cdna_position: Column = col("annotation.cDNA_position") as "cdna_position"
    val protein_position: Column = col("annotation.Protein_position") as "protein_position"
    val amino_acids: Column = col("annotation.Amino_acids") as "amino_acids"
    val codons: Column = col("annotation.Codons") as "codons"
    val variant_class: Column = col("annotation.VARIANT_CLASS") as "variant_class"
    val hgvsg: Column = col("annotation.HGVSg") as "hgvsg"
    val original_canonical: Column = when(col("annotation.CANONICAL") === "YES", lit(true))
      .otherwise(lit(false)) as "original_canonical"
    val is_multi_allelic: Column = col("splitFromMultiAllelic") as "is_multi_allelic"
    val old_multi_allelic: Column = col("INFO_OLD_MULTIALLELIC") as "old_multi_allelic"
    val sortChromosome: Column = when(col("chromosome") === "X", 100).when(col("chromosome") === "Y", 101)
      .when(col("chromosome") === "M", 102)
      .otherwise(col("chromosome").cast("int")) as "sort_chromosome"

    def optional_info(df: DataFrame,
                      colName: String,
                      alias: String,
                      colType: String = "string"): Column =
      (if (df.columns.contains(colName)) col(colName) else lit(null).cast(colType)).as(alias)

    //the order matters, do not change it
    val locusColumnNames: List[String] = List("chromosome", "start", "reference", "alternate")

    val locus: List[Column] = locusColumnNames.map(col)
    val id: Column = sha1(concat(col("chromosome"), col("start"), col("reference"), col("alternate"))) as "id"

  }

  /**
   * Reads vcf files into dataframe and apply transformations:
   *  - split_multiallelics
   *  - optionally normalize_variants if a path to a reference genome is given
   *
   * @param input               where the vcf files are located
   * @param referenceGenomePath reference genome path. This path has to be local for each executors
   * @param spark               a Spark session
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

