package bio.ferlab.datalake.spark3.implicits

import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}


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
    def getBA1(colFreq:String = "external_frequencies"): Column = {

      require(df.columns.contains(colFreq), s"Column `$colFreq` is required for BA1.")

      val afCols = df.select(s"$colFreq.*").columns.map { field =>
        struct(col(s"$colFreq.$field.af") as "v", lit(field) as "k")
      }

      val maxAf = greatest(afCols: _*).getItem("v")
      val study = greatest(afCols: _*).getItem("k")

      struct(
        study.as("study"),
        maxAf.as("max_af"),
        (maxAf >= 0.05).as("score")
      )

    }

  }

}
