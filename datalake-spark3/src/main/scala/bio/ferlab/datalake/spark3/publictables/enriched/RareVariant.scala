package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.utils.RepartitionByRange
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import java.time.LocalDateTime

class RareVariant()(implicit conf: Configuration) extends ETLSingleDestination {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_rare_variant")
  val gnomad_genomes_v2_1: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v2_1_1")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      gnomad_genomes_v2_1.id -> gnomad_genomes_v2_1.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    data(gnomad_genomes_v2_1.id)
      .select(columns.locus :+ col("af"): _*)
      .groupByLocus()
      .agg(max("af") as "af")
      .withColumn("is_rare", col("af") <= 0.01)
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(60))

}
