package bio.ferlab.datalake.spark3.implicits

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

object ACMGImplicits {

  val variantColumns = Array("chromosome", "start", "end", "reference", "alternate")

  private def validateRequiredColumns(map: Map[DataFrame, (String, Array[String])], criteriaName: String = "criteria"): Unit = {
    map.foreach {
      case (df, (dfName, columns)) => columns.foreach(
        col => require(
          df.columns.contains(col),
          s"Column `$col` is required in DataFrame $dfName for $criteriaName.")
      )
    }
  }

  /**
   * inColArray
   * Anonymous helper function generating boolean columns for array-containing columns.
   *
   * Designed to be used in a withColumn statement. Checks whether an entry in the column contains
   * at least one value from values argument.
   *
   * @return A Column of boolean
   */
  val inColArray = (colName: String, values: List[String]) => values.map(m => array_contains(col(colName), m)).reduce(_ || _)

  implicit class ACMGOperations(df: DataFrame) {

    /**
     * BA1 - ACMG criteria
     * Stand-alone evidence of benign impact.
     * Allele frequency is >5% in a large-scale sequencing project (1000 Genomes project, TOPMed BRAVO, gnomAD)
     *
     * Input DataFrame must contain an `external_frequencies` column.
     *
     * @return A struct containing the study with the highest AF, the AF and the BA1 score :
     *         StructType(
     *         StructField(study, StringType, true),
     *         StructField(max_af, DoubleType, true),
     *         StructField(score, BooleanType, true)
     *         )
     *
     */
    def getBA1(colFreq: String = "external_frequencies"): Column = {

      require(df.columns.contains(colFreq), s"Column `$colFreq` is required for BA1.")

      df.schema("external_frequencies").dataType match {
        case s: StructType =>
          val afCols = s.fields.map(_.name).map { field =>
            struct(col(s"$colFreq.$field.af") as "v", lit(field) as "k")
          }
          val maxAf = greatest(afCols: _*).getItem("v")
          val study = greatest(afCols: _*).getItem("k")

          struct(
            study.as("cohort"),
            maxAf.as("max_af"),
            (maxAf >= 0.05).as("score")
          )


        case _ => throw new IllegalArgumentException(s"Column `$colFreq` must be a StructType.")
      }
    }

    def getBS2(orphanet: DataFrame, frequencies: DataFrame): DataFrame = {

      val map = Map(
        df -> ("df", Array("symbol") ++ variantColumns),
        orphanet -> ("orphanet", Array("gene_symbol", "average_age_of_onset", "type_of_inheritance")),
        frequencies -> ("frequencies", Array("external_frequencies", "genes_symbol") ++ variantColumns)
      )
      validateRequiredColumns(map, "PM2")

      val threshold = 4

      val onsets = List(
        "Adult",
        "Elderly",
        "All ages",
        "No data available")

      val is_dominant_inheritance = List(
        "Autosomal dominant",
        "X-linked dominant",
        "Y-linked",
        "Mitochondrial inheritance")

      val orphanetDF = orphanet.select("gene_symbol", "average_age_of_onset", "type_of_inheritance")
        .withColumn("is_adult_onset", inColArray("average_age_of_onset", onsets))
        .filter(col("is_adult_onset") === false)
        .withColumn("is_dominant", inColArray("type_of_inheritance", is_dominant_inheritance))
        .select(
          col("gene_symbol").as("symbol"),
          col("is_dominant"))
        .distinct()

      val freqDF = frequencies
        .select(
          col("chromosome"),
          col("start"),
          col("end"),
          col("reference"),
          col("alternate"),
          explode(col("genes_symbol")).as("symbol"),
          col("external_frequencies.gnomad_genomes_3_1_1.ac").as("gnomad_ac"),
          col("external_frequencies.gnomad_genomes_3_1_1.hom").as("gnomad_hom"))

      df
        .join(orphanetDF, Seq("symbol"), "leftouter")
        .na.fill(false, Seq("is_dominant"))
        .join(freqDF, Seq("chromosome", "start", "end", "reference", "alternate", "symbol"), "leftouter")
        .withColumn("BS2", struct(
          col("gnomad_ac"),
          col("gnomad_hom"),
          col("is_dominant"),
          (
            col("gnomad_hom").isNotNull &&
              (
                col("gnomad_hom") >= threshold ||
                  (col("is_dominant") && col("gnomad_ac").isNotNull && col("gnomad_ac") >= threshold)
                )
            ).as("score")
        ))
        .drop("gnomad_ac", "gnomad_hom", "is_dominant")

    }

    def getPP2(clinvar: DataFrame): DataFrame = {

      val pathogenicClinSig = List("Pathogenic", "Likely_pathogenic")
      val benignClinSig = List("Benign", "Likely_benign")

      val clinvarDF = clinvar
        .filter(array_contains(col("mc"), "missense_variant") === true)
        .withColumn("is_pathogenic", inColArray("clin_sig", pathogenicClinSig))
        .withColumn("is_benign", inColArray("clin_sig", benignClinSig))
        .filter((col("is_pathogenic") !== col("is_benign")) === true)
        .withColumn("symbol", explode(split(col("geneinfo"), "\\|")))
        .groupBy("symbol").agg(
          sum(col("is_pathogenic").cast("int")).alias("n_pathogenic"),
          sum(col("is_benign").cast("int")).alias("n_benign")
        )
        .withColumn("is_missense_pathogenic", col("n_pathogenic") >= 3 && col("n_pathogenic") > col("n_benign") * 2)
        .withColumn("symbol", split(col("symbol"), ":").getItem(0))


      df.join(clinvarDF, Seq("symbol"), "leftouter")
        .na.fill(0, Seq("n_benign", "n_pathogenic"))
        .na.fill(false, Seq("is_missense_pathogenic"))
        .filter(array_contains(col("consequences"), "downstream_gene_variant") === false)
        .filter(col("symbol") !== "AARS1")
        .withColumn("PP2", struct(
          col("n_benign"),
          col("n_pathogenic"),
          col("is_missense_pathogenic"),
          (
            col("is_missense_pathogenic").isNotNull &&
              col("is_missense_pathogenic") === true &&
              array_contains(col("consequences"), "missense_variant")
            ).as("score")
        )).drop("n_pathogenic", "n_benign", "is_missense_pathogenic")
    }

  }

}
