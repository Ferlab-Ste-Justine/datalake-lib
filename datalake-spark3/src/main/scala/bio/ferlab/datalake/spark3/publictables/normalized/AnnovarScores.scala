package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.transformation.Cast.{castFloat, castLong}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class AnnovarScores(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_dbnsfp_annovar")
  val raw_dbnsfp_annovar: DatasetConf = conf.getDataset("raw_dbnsfp_annovar")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(raw_dbnsfp_annovar.id -> raw_dbnsfp_annovar.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    data(raw_dbnsfp_annovar.id)
      .select(
        $"#Chr" as "chromosome",
        castLong("Start") as "start",
        castLong("End") as "end",
        $"Ref" as "reference",
        $"Alt" as "alternate",
        $"DamagePredCount",
        castFloat("SIFT_score"),
        castFloat("SIFT_converted_rankscore"),
        $"SIFT_pred",
        castFloat("SIFT4G_score"),
        castFloat("SIFT4G_converted_rankscore"),
        $"SIFT4G_pred",
        castFloat("Polyphen2_HDIV_score"),
        castFloat("Polyphen2_HDIV_rankscore"),
        $"Polyphen2_HDIV_pred",
        castFloat("Polyphen2_HVAR_score"),
        castFloat("Polyphen2_HVAR_rankscore"),
        $"Polyphen2_HVAR_pred",
        castFloat("LRT_score"),
        castFloat("LRT_converted_rankscore"),
        $"LRT_pred",
        castFloat("MutationTaster_score"),
        castFloat("MutationTaster_converted_rankscore"),
        $"MutationTaster_pred",
        $"MutationAssessor_pred",
        castFloat("FATHMM_score"),
        castFloat("FATHMM_converted_rankscore"),
        $"FATHMM_pred",
        castFloat("PROVEAN_score"),
        castFloat("PROVEAN_converted_rankscore"),
        $"PROVEAN_pred",
        castFloat("VEST4_score"),
        castFloat("VEST4_rankscore"),
        castFloat("MetaSVM_score"),
        castFloat("MetaSVM_rankscore"),
        $"MetaSVM_pred",
        castFloat("MetaLR_score"),
        castFloat("MetaLR_rankscore"),
        $"MetaLR_pred",
        castFloat("MetaRNN_score"),
        castFloat("MetaRNN_rankscore"),
        $"MetaRNN_pred",
        castFloat("M-CAP_score"),
        castFloat("M-CAP_rankscore"),
        $"M-CAP_pred",
        castFloat("REVEL_score"),
        castFloat("REVEL_rankscore"),
        castFloat("MutPred_score"),
        castFloat("MutPred_rankscore"),
        castFloat("MVP_score"),
        castFloat("MVP_rankscore"),
        castFloat("MPC_score"),
        castFloat("MPC_rankscore"),
        castFloat("PrimateAI_score"),
        castFloat("PrimateAI_rankscore"),
        $"PrimateAI_pred",
        castFloat("DEOGEN2_score"),
        castFloat("DEOGEN2_rankscore"),
        $"DEOGEN2_pred",
        castFloat("BayesDel_addAF_score"),
        castFloat("BayesDel_addAF_rankscore"),
        $"BayesDel_addAF_pred",
        castFloat("BayesDel_noAF_score"),
        castFloat("BayesDel_noAF_rankscore"),
        $"BayesDel_noAF_pred",
        castFloat("ClinPred_score"),
        castFloat("ClinPred_rankscore"),
        $"ClinPred_pred",
        castFloat("LIST-S2_score"),
        castFloat("LIST-S2_rankscore"),
        $"LIST-S2_pred",
        $"Aloft_pred",
        $"Aloft_Confidence",
        castFloat("CADD_raw"),
        castFloat("CADD_raw_rankscore"),
        $"CADD_phred",
        castFloat("DANN_score"),
        castFloat("DANN_rankscore"),
        castFloat("fathmm-MKL_coding_score"),
        castFloat("fathmm-MKL_coding_rankscore"),
        $"fathmm-MKL_coding_pred",
        castFloat("fathmm-XF_coding_score"),
        castFloat("fathmm-XF_coding_rankscore"),
        $"fathmm-XF_coding_pred",
        castFloat("Eigen-raw_coding"),
        castFloat("Eigen-raw_coding_rankscore"),
        castFloat("Eigen-PC-raw_coding"),
        castFloat("Eigen-PC-raw_coding_rankscore"),
        castFloat("GenoCanyon_score"),
        castFloat("GenoCanyon_rankscore"),
        castFloat("integrated_fitCons_score"),
        castFloat("integrated_fitCons_rankscore"),
        $"integrated_confidence_value",

        castFloat("LINSIGHT"),
        castFloat("LINSIGHT_rankscore"),
        castFloat("GERP++_NR"),
        castFloat("GERP++_RS"),
        castFloat("GERP++_RS_rankscore"),
        castFloat("phyloP100way_vertebrate"),
        castFloat("phyloP100way_vertebrate_rankscore"),
        castFloat("phyloP30way_mammalian"),
        castFloat("phyloP30way_mammalian_rankscore"),
        castFloat("phastCons100way_vertebrate"),
        castFloat("phastCons100way_vertebrate_rankscore"),
        castFloat("phastCons30way_mammalian"),
        castFloat("phastCons30way_mammalian_rankscore"),
        castFloat("SiPhy_29way_logOdds"),
        castFloat("SiPhy_29way_logOdds_rankscore"),
        $"Interpro_domain",
        split($"GTEx_V8_gene", ";") as "GTEx_V8_gene",
        split($"GTEx_V8_tissue", ";") as "GTEx_V8_tissue",
        castLong("TWINSUK_AC"),
        castFloat("TWINSUK_AF"),
        castLong("ALSPAC_AC"),
        castFloat("ALSPAC_AF"),
        castLong("UK10K_AC"),
        castFloat("UK10K_AF"),
      )
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chomosome", "start"), n = Some(40))

}


object AnnovarScores {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    AnnovarScores(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
