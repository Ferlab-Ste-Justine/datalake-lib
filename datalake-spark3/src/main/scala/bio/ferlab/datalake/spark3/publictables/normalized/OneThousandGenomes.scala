package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class OneThousandGenomes(rc: RuntimeETLContext) extends SimpleETLP(rc) {
  private val raw_1000_genomes = conf.getDataset("raw_1000_genomes")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_1000_genomes")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(raw_1000_genomes.id -> raw_1000_genomes.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(raw_1000_genomes.id)
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
        afr_af,
        eur_af,
        sas_af,
        amr_af,
        eas_af,
        dp
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq("start"))
}

object OneThousandGenomes {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    OneThousandGenomes(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
