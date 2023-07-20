package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.etl.{ETLContext, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class TopMed(rc: ETLContext) extends SimpleETLP(rc) {

  private val raw_topmed = conf.getDataset("raw_topmed_bravo")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_topmed_bravo")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(raw_topmed.id -> raw_topmed.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
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
        $"INFO_HOM"(0) as "homozygotes",
        $"INFO_HET"(0) as "heterozygotes",
        $"qual",
        when(size($"filters") === 1 && $"filters"(0) === "PASS", "PASS")
          .when(array_contains($"filters", "PASS"), "PASS+FAIL")
          .otherwise("FAIL") as "qual_filter"
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq("start"))
}

object TopMed {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    TopMed(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}