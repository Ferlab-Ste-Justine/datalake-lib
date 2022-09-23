package bio.ferlab.datalake.spark3.public.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.utils.RepartitionByRange
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDateTime

class GnomadV3()(implicit conf: Configuration) extends ETLP {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_gnomadv3")
  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomadv3")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(gnomad_vcf.id -> gnomad_vcf.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
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
