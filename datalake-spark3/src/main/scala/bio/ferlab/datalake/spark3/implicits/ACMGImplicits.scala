package bio.ferlab.datalake.spark3.implicits

import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType}


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
     */
    def get_BA1: Column = {

      require(df.columns.contains("external_frequencies"), "Column `external_frequencies` is required for BA1.")

      val afCols = for (field <- df.select("external_frequencies.*").columns) yield s"external_frequencies.${field}.af"

      val structs = afCols.map(
        c => struct(col(c).as("v"), lit(c).as("k"))
      )

      val study = split(greatest(structs: _*).getItem("k"), "\\.")(1)
      val max_af = greatest(structs: _*).getItem("v")

      struct(
        study.as("study"),
        max_af.as("max_af"),
        (max_af >= 0.05).as("score")
      )

    }

  }

}
