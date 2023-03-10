package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.commons.utils.RepartitionByColumns
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
class TopMed()(implicit conf: Configuration) extends ETLP {

  private val raw_topmed = conf.getDataset("raw_topmed_bravo")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_topmed_bravo")
  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(raw_topmed.id -> raw_topmed.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(raw_topmed.id)
      .select(
        chromosome,
        start,
        end,
        name,
        reference,
        alternate,
        ac,
        af,
        an,
        $"INFO_HOM" (0) as "homozygotes",
        $"INFO_HET" (0) as "heterozygotes",
        $"qual",
        $"INFO_FILTERS" as "filters",
        when(size($"INFO_FILTERS") === 1 && $"INFO_FILTERS" (0) === "PASS", "PASS")
          .when(array_contains($"INFO_FILTERS", "PASS"), "PASS+FAIL")
          .otherwise("FAIL") as "qual_filter"
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq(col("start")))
}
