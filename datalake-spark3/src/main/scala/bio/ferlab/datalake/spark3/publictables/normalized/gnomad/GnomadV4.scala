package bio.ferlab.datalake.spark3.publictables.normalized.gnomad

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime

case class GnomadV4(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v4")
  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomad_genomes_v4")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(gnomad_vcf.id -> gnomad_vcf.read)
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
      $"ac".cast("long"),
      $"af",
      $"an".cast("long"),
      $"nhomalt".cast("long") as "hom"
    )
  }


  private def flattenInfo(df: DataFrame): Seq[Column] = {
    val replaceColumnName: String => String = name => name.replace("INFO_", "").toLowerCase

    df.schema.toList.collect {
      case c
        if (c.name.startsWith("INFO_AN") ||
          c.name.startsWith("INFO_AC") ||
          c.name.startsWith("INFO_AF") ||
          c.name.startsWith("INFO_nhomalt")) && c.dataType.isInstanceOf[ArrayType] =>
        col(c.name)(0) as replaceColumnName(c.name)
      case c if c.name.startsWith("INFO_") =>
        col(c.name) as replaceColumnName(c.name)
    }
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