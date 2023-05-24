package bio.ferlab.datalake.spark3.genomics


import bio.ferlab.datalake.spark3.implicits.FrequencyUtils.array_sum
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Frequencies {

  implicit class FrequencyOperations(df: DataFrame) {

    /**
     * Calculate frequencies on variants in DataFrame df and according to splits parameters.
     *
     * @param split List of frequencies to calculate
     * @return  A dataframe with one line per locus and frequencies columns specified by splits parameter.
     *
     *          Usage example :
     *          Using this dataframe as input :
     *          +----------+-----+---------+---------+------+---------------+--------+--------+---------+----------+
     *          |chromosome|start|reference|alternate|calls |affected_status|zygosity|study_id|ethnicity|patient_id|
     *          +----------+-----+---------+---------+------+---------------+--------+--------+---------+----------+
     *          |1         |1000 |A        |T        |[1, 1]|false          |HOM     |S1      |null     |P1        |
     *          |1         |1000 |A        |T        |[1, 1]|true           |HOM     |S1      |null     |P1        |
     *          |1         |1000 |A        |T        |[1, 1]|false          |HOM     |S2      |null     |P1        |
     *          +----------+-----+---------+---------+------+---------------+--------+--------+---------+----------+
     *          And then calculate frequencies with these parameters :
     *          val result = input.freq(
     *          FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id")), byAffected = true),
     *          FrequencySplit("frequency_kf", byAffected = true),
     *          )
     *          Resulting dataframe will contain all locus columns + 2 frequency columns : frequency_by_study_id and frequency_kf:
     *          +----------+-----+---------+---------+------------------------------------------------------------------------------------------------------------------+---------------------------------------------------+
     *          |chromosome|start|reference|alternate|frequency_by_study_id                                                                                             |frequency_kf                                       |
     *          +----------+-----+---------+---------+------------------------------------------------------------------------------------------------------------------+---------------------------------------------------+
     *          |1         |1000 |A        |T        |[{S2, {2, 2, 1, 1, 1}, {0, 0, 0, 0, 0}, {2, 2, 1, 1, 1}}, {S1, {4, 4, 2, 2, 2}, {2, 2, 1, 1, 1}, {2, 2, 1, 1, 1}}]|{{6, 6, 3, 3, 3}, {2, 2, 1, 1, 1}, {4, 4, 2, 2, 2}}|
     *          +----------+-----+---------+---------+------------------------------------------------------------------------------------------------------------------+---------------------------------------------------+
     *
     *          frequency_by_study_id :
     *          - induced by this split parameter FrequencySplit("frequency_by_study_id", splitBy = Some(col("study_id")))
     *          - is an array of struct, each struct represents a frequency  for a study_id. Fields of this struct are :
     *            * study_id (split column)
     *            * total frequency, which is also a struct that contains these fields : ac (allele count), an (allele number), pc (patient count), pn (patient number), hom (number of homozygous)
     *            * if byAffected parameter is set to true it contains also two others frequencies (affected and not_affected) which have the same fields as total
     *            frequency_kf :
     *          - induced by this split parameter FrequencySplit("frequency_kf")
     *          - is a struct of frequency. Fields of this struct are :
     *            * study_id (split column)
     *            * total frequency, which is also a struct that contains these fields : ac (allele count), an (allele number), pc (patient count), pn (patient number), hom (number of homozygous)
     *            * if byAffected parameter is set to true it contains also two others frequencies (affected and not_affected) which have the same fields as total
     *
     *          Here the schema of output dataframe :
     *          root
     *          |-- chromosome: string (nullable = true)
     *          |-- start: long (nullable = false)
     *          |-- reference: string (nullable = true)
     *          |-- alternate: string (nullable = true)
     *          |-- frequency_by_study_id: array (nullable = false)
     *          |    |-- element: struct (containsNull = false)
     *          |    |    |-- study_id: string (nullable = true)
     *          |    |    |-- total: struct (nullable = false)
     *          |    |    |    |-- ac: long (nullable = true)
     *          |    |    |    |-- an: long (nullable = true)
     *          |    |    |    |-- pn: long (nullable = true)
     *          |    |    |    |-- pc: long (nullable = true)
     *          |    |    |    |-- hom: long (nullable = true)
     *          |    |    |-- affected: struct (nullable = false)
     *          |    |    |    |-- ac: long (nullable = true)
     *          |    |    |    |-- an: long (nullable = true)
     *          |    |    |    |-- pn: long (nullable = true)
     *          |    |    |    |-- pc: long (nullable = true)
     *          |    |    |    |-- hom: long (nullable = true)
     *          |    |    |-- not_affected: struct (nullable = false)
     *          |    |    |    |-- ac: long (nullable = true)
     *          |    |    |    |-- an: long (nullable = true)
     *          |    |    |    |-- pn: long (nullable = true)
     *          |    |    |    |-- pc: long (nullable = true)
     *          |    |    |    |-- hom: long (nullable = true)
     *          |-- frequency_kf: struct (nullable = false)
     *          |    |-- total: struct (nullable = false)
     *          |    |    |-- ac: long (nullable = true)
     *          |    |    |-- an: long (nullable = true)
     *          |    |    |-- pn: long (nullable = true)
     *          |    |    |-- pc: long (nullable = true)
     *          |    |    |-- hom: long (nullable = true)
     *          |    |-- affected: struct (nullable = false)
     *          |    |    |-- ac: long (nullable = true)
     *          |    |    |-- an: long (nullable = true)
     *          |    |    |-- pn: long (nullable = true)
     *          |    |    |-- pc: long (nullable = true)
     *          |    |    |-- hom: long (nullable = true)
     *          |    |-- not_affected: struct (nullable = false)
     *          |    |    |-- ac: long (nullable = true)
     *          |    |    |-- an: long (nullable = true)
     *          |    |    |-- pn: long (nullable = true)
     *          |    |    |-- pc: long (nullable = true)
     *          |    |    |-- hom: long (nullable = true)
     *
     *
     */
    def freq(split: FrequencySplit*): DataFrame = {
      val allDataframes: Seq[DataFrame] = split.map { split =>
        val splitColumn: Seq[Column] = split.splitBy.toSeq
        if (split.byAffected) {
          val affected_status = col("affected_status")
          val firstSplit = df
            .groupByLocus(splitColumn :+ affected_status : _*)
            .agg(
              ifAffected(ac) as "affected_ac",
              ifAffected(an) as "affected_an",
              ifAffected(pc) as "affected_pc",
              ifAffected(pn) as "affected_pn",
              ifAffected(hom) as "affected_hom",
              ifNotAffected(ac) as "not_affected_ac",
              ifNotAffected(an) as "not_affected_an",
              ifNotAffected(pc) as "not_affected_pc",
              ifNotAffected(pn) as "not_affected_pn",
              ifNotAffected(hom) as "not_affected_hom"
            )
            .groupByLocus(splitColumn: _*)
            .agg(
              struct(sum("affected_ac") as "ac", sum("affected_an") as "an", sum("affected_pn") as "pn", sum("affected_pc") as "pc", sum("affected_hom") as "hom") as "affected",
              struct(sum(col("not_affected_ac")) as "ac", sum(col("not_affected_an")) as "an", sum("not_affected_pn") as "pn", sum("not_affected_pc") as "pc", sum("not_affected_hom") as "hom") as "not_affected",
            ).withColumn("total",
            struct(
              col("affected.ac") + col("not_affected.ac") as "ac",
              col("affected.an") + col("not_affected.an") as "an",
              col("affected.pn") + col("not_affected.pn") as "pn",
              col("affected.pc") + col("not_affected.pc") as "pc",
              col("affected.hom") + col("not_affected.hom") as "hom",
            ))
          split.splitBy.map { s =>
            firstSplit
              .groupByLocus()
              .agg(collect_list(struct(s, col("total"), col("affected"), col("not_affected"))) as split.name)
          }.getOrElse {
            firstSplit
              .withColumn(split.name, struct(col("total"), col("affected"), col("not_affected")))
              .drop("total", "affected", "not_affected")
          }
        } else {
          val firstSplit =
            df.groupByLocus(splitColumn: _*)
              .agg(struct(ac, an, pc, pn, hom) as "total")
          split.splitBy.map {
            s =>
              firstSplit
                .groupByLocus()
                .agg(collect_list(struct(s, col("total"))) as split.name)

          }.getOrElse(
            firstSplit
              .withColumn(split.name, struct(col("total")))
              .drop("total")
          )
        }


      }
      allDataframes.reduce((df1, df2) => df1.joinByLocus(df2, "inner"))

    }
  }

  private def ifAffected(c: Column) = when(col("affected_status"), c).otherwise(0)

  private def ifNotAffected(c: Column) = when(not(col("affected_status")), c).otherwise(0)

  /**
   * allele count
   */
  private val ac: Column = sum(array_sum(filter(col("calls"), c => c === 1))) as "ac"

  /**
   * allele total number
   */
  private val an: Column = sum(lit(2)) as "an"

  /**
   * participant count
   */
  private val pc: Column = sum(when(array_contains(col("calls"), 1), 1).otherwise(0)) as "pc"

  /**
   * participant total number
   */
  private val pn: Column = sum(lit(1)) as "pn"

  /**
   * Number of homozygous
   */
  private val hom: Column = sum(when(col("zygosity") === "HOM", 1).otherwise(0)) as "hom"
}

/**
 * Represents a split for a frequency.
 *
 * @param name       column name used as an output for this split
 * @param filter     column used to filter input dataframe
 * @param splitBy    column used to split frquencies
 * @param byAffected flag that indicates if we need to calculate disticnt frequencies by affected and not affected
 */
case class FrequencySplit(name: String, filter: Option[Column] = None, splitBy: Option[Column] = None, byAffected: Boolean = false)