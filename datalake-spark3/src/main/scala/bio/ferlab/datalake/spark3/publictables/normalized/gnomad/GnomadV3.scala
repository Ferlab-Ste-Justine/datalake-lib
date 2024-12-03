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

case class GnomadV3(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_gnomad_genomes_v3")
  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomad_genomes_v3")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(gnomad_vcf.id -> gnomad_vcf.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val df = data(gnomad_vcf.id)

    df
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
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))


  /* in case we decide to load the files one at a time
   *
  override def run(runType: RunType)(implicit spark: SparkSession): DataFrame = {
    //clears the existing data
    HadoopFileSystem.remove(destination.location)
    //for each file found in /raw/gnomad/r3.1.1/
    HadoopFileSystem
      .list(Raw.gnomad_genomes_3_1_1.location, recursive = true)
      .filter(_.name.endsWith(".vcf.gz"))
      .foreach { f =>
        println(s"processing ${f.path}")
        val input = Map(Raw.gnomad_genomes_3_1_1.id -> vcf(f.path))
        load(transform(input))
        println(s"Done")
      }
    spark.emptyDataFrame
  }
   */
}

object GnomadV3 {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    GnomadV3(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}