package bio.ferlab.datalake.spark3.publictables.normalized.gnomad

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class GnomadV4SV(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_gnomad_sv_v4")
  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomad_sv_v4")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(gnomad_vcf.id -> vcf(gnomad_vcf.location, None))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    /*
    * We drop INFO_END because once we use flattenInfo, two columns will be named `end`
    * After reviewing the original VCF, it seems that INFO_END is always null
    * */

    val df = data(gnomad_vcf.id)
      .drop("INFO_END")
      .withColumnRenamed("filters", "INFO_FILTERS")

    df.select(
        chromosome +:
        start +:
        end +:
        reference +:
        alternate +:
        $"qual" +:
        name +:
        flattenInfo(df): _*
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))

}

object GnomadV4SV {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    GnomadV4SV(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}