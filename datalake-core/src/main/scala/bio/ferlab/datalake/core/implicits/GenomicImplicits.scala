package bio.ferlab.datalake.core.implicits

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}

object GenomicImplicits {

  val id: Column = sha1(concat(col("chromosome"), col("start"), col("reference"), col("alternate"))) as "id"

  implicit class ByLocusDataframe(df: DataFrame) {

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

  }

}

