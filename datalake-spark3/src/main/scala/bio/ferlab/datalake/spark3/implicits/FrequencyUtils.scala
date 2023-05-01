package bio.ferlab.datalake.spark3.implicits

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object FrequencyUtils {

  def array_sum(c: Column): Column = aggregate(c, lit(0), (accumulator, item) => accumulator + item)

  val includeFilter: Column = col("ad_alt") >= 3

  val frequencyFilter: Column = array_contains(col("filters"), "PASS") && includeFilter && col("gq") >= 20

  /**
   * allele count
   */
  val ac: Column = sum(when(frequencyFilter, array_sum(filter(col("calls"), c => c === 1))).otherwise(0)) as "ac"

  /**
   * allele total number
   */
  val an: Column = sum(lit(2)) as "an" //alternate computation method: sum(size(col("calls"))) as "an"

  /**
   * participant count
   */
  val pc: Column = sum(when(array_contains(col("calls"), 1) and frequencyFilter, 1).otherwise(0)) as "pc"

  /**
   * participant total number
   */
  val pn: Column = sum(lit(1)) as "pn"

  val hom: Column = sum(when(col("zygosity") === "HOM" and frequencyFilter, 1).otherwise(0)) as "hom"
  val het: Column = sum(when(col("zygosity") === "HET" and frequencyFilter, 1).otherwise(0)) as "het"

}
