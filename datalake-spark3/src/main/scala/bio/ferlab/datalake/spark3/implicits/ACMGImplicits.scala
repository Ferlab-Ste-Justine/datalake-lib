package bio.ferlab.datalake.spark3.implicits

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

object ACMGImplicits {

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
    def getPM2(omimDF: DataFrame): Column = {

      val inheritance_modes = List(
        "Pseudoautosomal recessive",
        "Autosomal recessive",
        "Digenic recessive",
        "X-linked recessive")

      val _df = omim_df.select("symbols", "phenotype.inheritance")
        .withColumn("is_recessive", inheritance_modes.map(m => array_contains($"inheritance", m)).reduce(_ || _))
        .select($"is_recessive", explode($"symbols").as("gene_symbol"))
        .filter($"is_recessive" === true)

      lit(1)
    }

  }

}
