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

    /**
     * PM2 - ACMG criteria
     * Moderate (supporting) evidence of pathogenic impact.
     * Absent from controls (or at extremely low frequency if recessive) in Exome Sequencing Project, 1000 Genomes
     * Project, or Exome Aggregration Consortium.
     *
     * WIP
     *
     */
    def getPM2(omim: DataFrame, frequencies: DataFrame): DataFrame = {

      val map = Map(
        df -> ("df", Array("symbol") ++ variantColumns),
        omim -> ("omim", Array("symbols", "phenotype")),
        frequencies -> ("frequencies", Array("external_frequencies", "genes_symbol") ++ variantColumns)
      )
      validateRequiredColumns(map, "PM2")

      // Extracting inheritance, identifying recessive genes
      val inheritanceModes = List(
        "Pseudoautosomal recessive",
        "Autosomal recessive",
        "Digenic recessive",
        "X-linked recessive")

      val omimRecessive = omim.select("symbols", "phenotype.inheritance")
        .withColumn("is_recessive", inheritanceModes.map(m => array_contains(col("inheritance"), m)).reduce(_ || _))
        .select(col("is_recessive"), explode(col("symbols")).as("symbol"))
        .filter(col("is_recessive") === true)
        .distinct()


      // Extracting frequency (and lack of)
      val maxAf = frequencies.schema("external_frequencies").dataType match {
        case s: StructType => {
          val afCols = s.fields.map(_.name).map { field =>
            struct(col(s"external_frequencies.$field.af") as "v", lit(field) as "k")
          }
          greatest(afCols: _*).getItem("v").as("max_af")
        }
      }

      val freqPerSymbol = frequencies.select(
        col("chromosome"),
        col("start"),
        col("end"),
        col("reference"),
        col("alternate"),
        explode(col("genes_symbol")).as("symbol"),
        maxAf,
        maxAf.isNull.as("max_af_is_null")
      )

      df
        .join(omimRecessive, Seq("symbol"), "leftouter")
        .na.fill(false, Seq("is_recessive"))
        .join(freqPerSymbol, Seq("chromosome", "start", "end", "reference", "alternate", "symbol"), "leftouter")
        .na.fill(false, Seq("max_af_is_null"))
        .na.fill(0, Seq("max_af"))
        .withColumn("PM2",
          struct(
            col("is_recessive").as("is_recessive"),
            col("max_af").as("max_af"),
            col("max_af_is_null").as("max_af_is_null"),
            (col("max_af_is_null") ||
              col("max_af") === 0 ||
              (col("is_recessive") && col("max_af") < 0.0001)).as("score")
          )
        )
        .drop("is_recessive", "max_af", "max_af_is_null")

    }
  }
}
