package bio.ferlab.datalake.spark3.genomics


import bio.ferlab.datalake.spark3.implicits.FrequencyUtils.array_sum
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locus
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row}

object Splits {

  implicit class SplitOperations(df: DataFrame) {

    /**
     * Calculate frequencies and simple splits on variants in DataFrame df.
     *
     * @param participantId  This column is used to determine number of distinct participants (pn) that has been sequenced
     * @param affectedStatus This column is used to calculate frequencies by affected status.
     * @param splits         List of splits to calculate, can contain both simple splits and frequency splits
     * @return A dataframe with one line per locus and split columns specified by splits parameter.
     * @example
     *          Using this dataframe as input :
     * {{{
     *+----------+-----+-----+---------+---------+------+---------------+------------+---------------+----+-------------+------------+--------+--------+---------+--------------+-----------------+------------+
     *|chromosome|start|end  |reference|alternate|calls |affected_status|genes_symbol|hgvsg          |name|variant_class|variant_type|zygosity|study_id|ethnicity|participant_id|transmission_mode|study_code  |
     *+----------+-----+-----+---------+---------+------+---------------+------------+---------------+----+-------------+------------+--------+--------+---------+--------------+-----------------+------------+
     *|1         |69897|69898|T        |C        |[1, 1]|false          |[OR4F5]     |chr1:g.69897T>C|null|SNV          |germline    |HOM     |S1      |null     |P1            |AR               |STUDY_CODE_1|
     *|1         |69897|69898|T        |C        |[0, 1]|true           |[OR4F5]     |chr1:g.69897T>C|null|SNV          |germline    |HET     |S1      |null     |P2            |AD               |STUDY_CODE_1|
     *|1         |69897|69898|T        |C        |[1, 1]|false          |[OR4F5]     |chr1:g.69897T>C|null|SNV          |germline    |HOM     |S2      |null     |P3            |AR               |STUDY_CODE_2|
     *|2         |69897|69898|T        |C        |[1, 1]|false          |[OR4F5]     |chr1:g.69897T>C|null|SNV          |germline    |HOM     |S2      |null     |P4            |AR               |STUDY_CODE_2|
     *+----------+-----+-----+---------+---------+------+---------------+------------+---------------+----+-------------+------------+--------+--------+---------+--------------+-----------------+------------+
     * }}}
     *
     *          And then calculate frequencies with these parameters :
     * {{{
     *  val result = input.split(
     *    FrequencySplit("frequency_by_study_id", extraSplitBy = Some(col("study_id")), byAffected = true, extraAggregations = Seq(
     *        AtLeastNElements(name = "participant_ids", c = col("participant_id"), n = 2),
     *        SimpleAggregation(name = "transmissions", c = col("transmission_mode")),
     *        FirstElement(name = "study_code", col("study_code"))
     *      )
     *    ),
     *    FrequencySplit("frequency_kf", byAffected = true, extraAggregations = Seq(SimpleAggregation(name = "zygosities", c = col("zygosity"))))
     *)
     * }}}
     *
     *
     *          Resulting dataframe will contain all locus columns + 2 frequency columns : frequency_by_study_id and frequency_kf:
     * {{{
     *+----------+-----+---------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
     *|chromosome|start|reference|alternate|frequency_by_study_id                                                                                                                                                                                                                      |frequency_kf                                                                                                                  |
     *+----------+-----+---------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
     *|2         |69897|T        |C        |[{S2, {2, 1, 1, 2, 4, 0.5, 0.5}, {0, 0, 0, 0, 0, 0.0, 0.0}, {2, 1, 1, 2, 4, 0.5, 0.5}, null, [AR], STUDY_CODE_2}]                                                                                                                          |{{2, 1, 1, 4, 8, 0.25, 0.25}, {0, 0, 0, 1, 2, 0.0, 0.0}, {2, 1, 1, 3, 6, 0.3333333333333333, 0.3333333333333333}, [HOM]}      |
     *|1         |69897|T        |C        |[{S2, {2, 1, 1, 2, 4, 0.5, 0.5}, {0, 0, 0, 0, 0, 0.0, 0.0}, {2, 1, 1, 2, 4, 0.5, 0.5}, null, [AR], STUDY_CODE_2}, {S1, {3, 2, 1, 2, 4, 0.75, 1.0}, {1, 1, 0, 1, 2, 0.5, 1.0}, {2, 1, 1, 1, 2, 1.0, 1.0}, [P1, P2], [AR, AD], STUDY_CODE_1}]|{{5, 3, 2, 4, 8, 0.625, 0.75}, {1, 1, 0, 1, 2, 0.5, 1.0}, {4, 2, 2, 3, 6, 0.6666666666666666, 0.6666666666666666}, [HOM, HET]}|
     *+----------+-----+---------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------+
     ** }}}
     *
     *
     *          - frequency_by_study_id :
     *            - induced by split parameter FrequencySplit("frequency_by_study_id", extraSplitBy = Some(col("study_id"))). See [[FrequencySplit]].
     *            - is an array of struct, each struct represents a frequency  for a study_id. Fields of this struct are :
     *              - study_id (split column)
     *              - total frequency, which is also a struct that contains these fields : ac (allele count), an (allele number), pc (patient count), pn (patient number), hom (number of homozygous), af (allele frequency), pf (participant frequency)
     *              - if byAffected parameter is set to true it contains also two others frequencies (affected and not_affected) which have the same fields as total
     *              - participant_ids : extra aggregation obtained by AtLeastNElements("participant_ids", col("participant_id"), 2). See [[AtLeastNElements]].
     *              - transmissions : extra aggregation obtained by SimpleAggregation("transmissions", col("transmission_mode")). See [[SimpleAggregation]].
     *
     *          - frequency_kf :
     *            - induced by this split parameter FrequencySplit("frequency_kf")
     *            - is a struct of frequency. Fields of this struct are :
     *              - study_id (split column)
     *              - total frequency, which is also a struct that contains these fields : ac (allele count), an (allele number), pc (patient count), pn (patient number), hom (number of homozygous), af (allele frequency), pf (participant frequency)
     *              - if byAffected parameter is set to true it contains also two others frequencies (affected and not_affected) which have the same fields as total
     *              - zygosities : extra aggregation obtained by SimpleAggregation(name = "zygosities", c = col("zygosity")). See [[SimpleAggregation]].
     *
     *
     *          Here the schema of output dataframe :
     * {{{
     *root
     *|-- chromosome: string (nullable = true)
     *|-- start: long (nullable = false)
     *|-- reference: string (nullable = true)
     *|-- alternate: string (nullable = true)
     *|-- frequency_by_study_id: array (nullable = false)
     *|    |-- element: struct (containsNull = false)
     *|    |    |-- study_id: string (nullable = true)
     *|    |    |-- total: struct (nullable = false)
     *|    |    |    |-- ac: long (nullable = true)
     *|    |    |    |-- pc: long (nullable = true)
     *|    |    |    |-- hom: long (nullable = true)
     *|    |    |    |-- pn: long (nullable = true)
     *|    |    |    |-- an: long (nullable = true)
     *|    |    |    |-- af: double (nullable = true)
     *|    |    |    |-- pf: double (nullable = true)
     *|    |    |-- affected: struct (nullable = false)
     *|    |    |    |-- ac: long (nullable = true)
     *|    |    |    |-- pc: long (nullable = true)
     *|    |    |    |-- hom: long (nullable = true)
     *|    |    |    |-- pn: long (nullable = true)
     *|    |    |    |-- an: long (nullable = true)
     *|    |    |    |-- af: double (nullable = true)
     *|    |    |    |-- pf: double (nullable = true)
     *|    |    |-- not_affected: struct (nullable = false)
     *|    |    |    |-- ac: long (nullable = true)
     *|    |    |    |-- pc: long (nullable = true)
     *|    |    |    |-- hom: long (nullable = true)
     *|    |    |    |-- pn: long (nullable = true)
     *|    |    |    |-- an: long (nullable = true)
     *|    |    |    |-- af: double (nullable = true)
     *|    |    |    |-- pf: double (nullable = true)
     *|    |    |-- participant_ids: array (nullable = true)
     *|    |    |    |-- element: string (containsNull = false)
     *|    |    |-- transmissions: array (nullable = false)
     *|    |    |    |-- element: string (containsNull = false)
     *|    |    |-- study_code: string (nullable = true)
     *|-- frequency_kf: struct (nullable = false)
     *|    |-- total: struct (nullable = false)
     *|    |    |-- ac: long (nullable = true)
     *|    |    |-- pc: long (nullable = true)
     *|    |    |-- hom: long (nullable = true)
     *|    |    |-- pn: long (nullable = false)
     *|    |    |-- an: long (nullable = false)
     *|    |    |-- af: double (nullable = true)
     *|    |    |-- pf: double (nullable = true)
     *|    |-- affected: struct (nullable = false)
     *|    |    |-- ac: long (nullable = true)
     *|    |    |-- pc: long (nullable = true)
     *|    |    |-- hom: long (nullable = true)
     *|    |    |-- pn: long (nullable = false)
     *|    |    |-- an: long (nullable = false)
     *|    |    |-- af: double (nullable = true)
     *|    |    |-- pf: double (nullable = true)
     *|    |-- not_affected: struct (nullable = false)
     *|    |    |-- ac: long (nullable = true)
     *|    |    |-- pc: long (nullable = true)
     *|    |    |-- hom: long (nullable = true)
     *|    |    |-- pn: long (nullable = false)
     *|    |    |-- an: long (nullable = false)
     *|    |    |-- af: double (nullable = true)
     *|    |    |-- pf: double (nullable = true)
     *|    |-- zygosities: array (nullable = false)
     *|    |    |-- element: string (containsNull = false)
     * }}}
     *
     */
    def split(participantId: Column = col("participant_id"), affectedStatus: Column = col("affected_status"), splits: Seq[OccurrenceSplit]): DataFrame = {

      val allDataframes: Seq[DataFrame] = splits.map { split =>
        val filteredDf: DataFrame = split.filter.fold(df)(f => df.filter(f))
        val splitColumns: Seq[Column] = split.extraSplitBy.fold(locus)(s => locus :+ s)
        val filterAggCols: Seq[OccurrenceAggregation] => List[Column] = aggs => aggs.map(agg => agg.filter(col(agg.name)) as agg.name).toList

        split match {
          case FrequencySplit(name, extraSplitBy, _, byAffected, extraAggregations) =>
            val extraFilteredAggCols: List[Column] = filterAggCols(extraAggregations)

            if (byAffected) {
              val firstSplit = filteredDf
                .groupBy(splitColumns :+ affectedStatus: _*)
                .agg(
                  ifAffected(ac) as "affected_ac",
                  Seq(
                    ifAffected(pc) as "affected_pc",
                    ifAffected(hom) as "affected_hom",
                    ifNotAffected(ac) as "not_affected_ac",
                    ifNotAffected(pc) as "not_affected_pc",
                    ifNotAffected(hom) as "not_affected_hom")
                    ++ extraAggregations.map(_.agg())
                    : _*
                )
                .groupBy(splitColumns: _*)
                .agg(
                  struct(sum("affected_ac") as "ac", sum("affected_pc") as "pc", sum("affected_hom") as "hom") as "affected",
                  Seq(
                    struct(sum(col("not_affected_ac")) as "ac", sum("not_affected_pc") as "pc", sum("not_affected_hom") as "hom") as "not_affected"
                  ) ++ extraAggregations.map(_.aggArray)
                    : _*

                ).withColumn("total",
                struct(
                  col("affected.ac") + col("not_affected.ac") as "ac",
                  col("affected.pc") + col("not_affected.pc") as "pc",
                  col("affected.hom") + col("not_affected.hom") as "hom",
                ))
              extraSplitBy.map { s =>
                val anPnDF = filteredDf.groupBy(s, affectedStatus).agg(
                  ifAffected(countDistinct(participantId)) as "pn_affected",
                  ifNotAffected(countDistinct(participantId)) as "pn_not_affected"
                )
                  .groupBy(s)
                  .agg(
                    sum("pn_affected") as "pn_affected",
                    sum("pn_not_affected") as "pn_not_affected",
                  )
                  .withColumn("pn", col("pn_affected") + col("pn_not_affected"))
                  .withColumn("an_affected", col("pn_affected") * 2)
                  .withColumn("an_not_affected", col("pn_not_affected") * 2)
                  .withColumn("an", col("pn") * 2)
                  .withColumn("joinSplit", s)
                  .drop(s)
                val frame = firstSplit
                  .withColumn("joinSplit", s)
                  .join(anPnDF, Seq("joinSplit"))
                  .withColumn("total", col("total").withField("pn", col("pn")).withField("an", col("an")))
                  .withColumn("affected", col("affected").withField("pn", col("pn_affected")).withField("an", col("an_affected")))
                  .withColumn("not_affected", col("not_affected").withField("pn", col("pn_not_affected")).withField("an", col("an_not_affected")))
                  .drop("joinSplit")
                frame
                  .groupByLocus()
                  .agg(
                    collect_list(
                      struct(
                        s ::
                          afpf("total") ::
                          afpf("affected") ::
                          afpf("not_affected") ::
                          extraFilteredAggCols: _*
                      )
                    ) as name
                  )
              }.getOrElse {
                val anPnDF = filteredDf.groupBy(affectedStatus).agg(
                  ifAffected(countDistinct(participantId)) as "pn_affected",
                  ifNotAffected(countDistinct(participantId)) as "pn_not_affected"
                )
                  .select(
                    sum("pn_affected") as "pn_affected",
                    sum("pn_not_affected") as "pn_not_affected",
                  )
                  .withColumn("pn", col("pn_affected") + col("pn_not_affected"))
                  .withColumn("an_affected", col("pn_affected") * 2)
                  .withColumn("an_not_affected", col("pn_not_affected") * 2)
                  .withColumn("an", col("pn") * 2)
                  .collect()
                  .head

                firstSplit
                  .withColumn("total", col("total").withField("pn", lit(anPnDF.getAs[Long]("pn"))).withField("an", lit(anPnDF.getAs[Long]("an"))))
                  .withColumn("affected", col("affected").withField("pn", lit(anPnDF.getAs[Long]("pn_affected"))).withField("an", lit(anPnDF.getAs[Long]("an_affected"))))
                  .withColumn("not_affected", col("not_affected").withField("pn", lit(anPnDF.getAs[Long]("pn_not_affected"))).withField("an", lit(anPnDF.getAs[Long]("an_not_affected"))))
                  .withColumn(name, struct(
                    afpf("total") ::
                      afpf("affected") ::
                      afpf("not_affected") ::
                      extraFilteredAggCols: _*)
                  )
                  .drop("total" :: "affected" :: "not_affected" :: extraAggregations.map(_.name).toList: _*)
              }
            } else {

              val firstSplit =
                filteredDf.groupBy(splitColumns: _*)
                  .agg(struct(ac, pc, hom) as "total", extraAggregations.map(_.agg()): _*)
              extraSplitBy.map { s =>
                val anPnDF = filteredDf.groupBy(s)
                  .agg(countDistinct(participantId) as "pn")
                  .withColumn("an", col("pn") * 2)
                  .withColumn("joinSplit", s)
                  .drop(s)
                firstSplit
                  .withColumn("joinSplit", s)
                  .join(anPnDF, Seq("joinSplit"))
                  .drop("joinSplit")
                  .withColumn("total", col("total").withField("pn", col("pn")).withField("an", col("an")))
                  .groupByLocus()
                  .agg(
                    collect_list(
                      struct(s :: afpf("total") :: extraFilteredAggCols: _*)
                    ) as name
                  )

              }.getOrElse {
                val anPnRow: Row = filteredDf.select(countDistinct(participantId) as "pn").withColumn("an", col("pn") * 2).collect().head
                val pn = anPnRow.getLong(0)
                val an = anPnRow.getLong(1)
                firstSplit
                  .withColumn("total", col("total").withField("pn", lit(pn)).withField("an", lit(an)))
                  .withColumn(name, struct(afpf("total") :: extraFilteredAggCols: _*) as name)
                  .drop("total" :: extraAggregations.map(_.name).toList: _*)
              }
            }

          case SimpleSplit(name, extraSplitBy, _, aggregations) =>
            val filteredAggCols: List[Column] = filterAggCols(aggregations)
            val firstSplit = filteredDf
              .groupBy(splitColumns: _*)
              .agg(aggregations.head.agg(), aggregations.tail.map(_.agg()): _*)

            extraSplitBy.map { s =>
              firstSplit
                .groupByLocus()
                .agg(collect_list(struct(s :: filteredAggCols: _*)) as name)

            }.getOrElse {
              firstSplit
                .withColumn(name, struct(filteredAggCols: _*))
                .drop(aggregations.map(_.name).toList: _*)
            }
        }
      }
      allDataframes.reduce((df1, df2) => df1.joinByLocus(df2, "full"))

    }
  }

  /**
   * Calculate af and pf values for a given struct column. af = ac / an and pf = pc / pn
   */
  private def afpf(columnName: String): Column = {
    val f = col(columnName)
    f
      .withField("af", when(f("an") === 0, 0.0).otherwise(f("ac") / f("an")))
      .withField("pf", when(f("pn") === 0, 0.0).otherwise(f("pc") / f("pn")))
      .alias(columnName)
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
 * Represents a split.
 *
 * @param name         column name used as an output for this split
 * @param extraSplitBy extra column to use with locus for the split
 * @param filter       column used to filter input dataframe
 */
sealed trait OccurrenceSplit {
  def name: String

  def extraSplitBy: Option[Column]

  def filter: Option[Column]
}

/**
 *
 * @param name              column name used as an output for this split
 * @param extraSplitBy      extra column to use with locus for the split
 * @param filter            column used to filter input dataframe
 * @param byAffected        flag that indicates if we need to calculate distinct frequencies by affected and not affected
 * @param extraAggregations optional extra aggregations to be calculated. See [[OccurrenceAggregation]].
 */
case class FrequencySplit(override val name: String, override val extraSplitBy: Option[Column] = None, override val filter: Option[Column] = None, byAffected: Boolean = false, extraAggregations: Seq[OccurrenceAggregation] = Nil) extends OccurrenceSplit

/**
 *
 * @param name         column name used as an output for this split
 * @param extraSplitBy extra column to use with locus for the split
 * @param filter       column used to filter input dataframe
 * @param aggregations aggregations to be calculated in the split. At least one must be specified. See [[OccurrenceAggregation]].
 */
case class SimpleSplit(override val name: String, override val extraSplitBy: Option[Column] = None, override val filter: Option[Column] = None, aggregations: Seq[OccurrenceAggregation]) extends OccurrenceSplit

/**
 * Represents an aggregation to be calculated.
 *
 * @param name   name of the aggregation
 * @param c      column to be aggregated
 * @param filter filter to be applied to the aggregation. Only aggregations matching the filter will be return. Otherwise it will be replaced by null. The default is no filter.
 */
trait OccurrenceAggregation {
  def name: String

  def c: Column

  def agg(column: Column = c): Column = collect_set(column) as name

  /**
   * How to aggregate when data is an array.
   *
   * @return
   */
  def aggArray: Column = array_distinct(flatten(agg(col(name)))) as name

  def filter: Column => Column = aggColumn => aggColumn
}

/**
 * Represents a simple aggregation that returns the collected set of column c from original dataframe.
 *
 * @param name name of the aggregation
 * @param c    column to be collected
 */
case class SimpleAggregation(override val name: String, override val c: Column) extends OccurrenceAggregation

/**
 * Represents an aggregation that returns the collected set of column c from original dataframe. It will return null if the size of the collected set is less than n.
 *
 * @param name name of the aggregation
 * @param c    column to be collected
 */
case class AtLeastNElements(override val name: String, override val c: Column, n: Int) extends OccurrenceAggregation {
  override val filter: Column => Column = aggColumn => when(size(aggColumn) >= n, aggColumn).otherwise(lit(null))
}

/**
 * Return only the first element of the aggregated column.
 *
 * @param name name of the aggregation
 * @param c    column to be aggregated
 */
case class FirstElement(override val name: String, override val c: Column) extends OccurrenceAggregation {
  override def agg(column: Column = c): Column = first(column) as name

  override def aggArray: Column = agg(col(name)) as name
}

