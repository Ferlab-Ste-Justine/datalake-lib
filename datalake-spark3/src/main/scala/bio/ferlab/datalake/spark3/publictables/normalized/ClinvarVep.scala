package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class ClinvarVep(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_clinvar_vep")

  val clinvar_vep_vcf: DatasetConf = conf.getDataset("raw_clinvar_vep")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    Map(clinvar_vep_vcf.id -> clinvar_vep_vcf.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime): DataFrame = {

    val df = data(clinvar_vep_vcf.id)

    df.select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        (col("INFO_CLNSIG") as "clin_sig"),
        col("INFO_CSQ") as "csq"
      )
      .withColumn(
        "clin_sig",
        split(regexp_replace(concat_ws("|", col("clin_sig")), "^_|\\|_|/", "|"), "\\|")
      )
      .withColumn("annotation", explode(col("csq")))
      .drop("csq")
      .select(
        col("chromosome"),
        col("start"),
        col("end"),
        col("reference"),
        col("alternate"),
        col("name"),
        col("clin_sig"),
        consequences,
        biotype,
        impact,
        symbol,
        ensembl_gene_id,
        ensembl_transcript_id,
        regexp_replace(hgvsp, "%3D", "=") as "hgvsp",
        hgvsc,
        hgvsg,
      )

  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()

}

object ClinvarVep {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    Clinvar(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

