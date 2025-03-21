package bio.ferlab.datalake.spark3.publictables.normalized.gnomad

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.vcf
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class GnomadV4(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_gnomad_joint_v4")
  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomad_joint_v4")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(gnomad_vcf.id -> vcf(gnomad_vcf.location, None))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val df = data(gnomad_vcf.id)

    val intermediate = df
      .select(
        chromosome +:
        start +:
        end +:
        reference +:
        alternate +:
        $"qual" +:
        name +:
        flattenInfo(df): _*
      )

    intermediate.select(
      $"chromosome",
      $"start",
      $"end",
      $"reference",
      $"alternate",
      $"qual",
      $"name",
      $"ac_joint".cast("long"),
      $"af_joint",
      $"an_joint".cast("long"),
      $"nhomalt_joint".cast("long") as "hom_joint",
      $"ac_genomes".cast("long"),
      $"af_genomes",
      $"an_genomes".cast("long"),
      $"nhomalt_genomes".cast("long") as "hom_genomes",
      $"ac_exomes".cast("long"),
      $"af_exomes",
      $"an_exomes".cast("long"),
      $"nhomalt_exomes".cast("long") as "hom_exomes",
    )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))

}

object GnomadV4 {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    GnomadV4(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}