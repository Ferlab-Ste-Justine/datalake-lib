package org.kidsfirstdrc.dwh.external.dbnsfp

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.RepartitionByRange
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.kidsfirstdrc.dwh.conf.Catalog.{Public, Raw}
import org.kidsfirstdrc.dwh.jobs.StandardETL

import java.time.LocalDateTime

class AnnovarScores()(implicit conf: Configuration) extends ETLP {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_dbnsfp_annovar")
  val raw_dbnsfp_annovar: DatasetConf = conf.getDataset("raw_dbnsfp_annovar")

  def cast(colName: String): Column = col(colName).cast(FloatType) as colName

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    //      .option("sep", "\t")
    //      .option("header", "true")
    //      .option("nullValue", ".")
    //      .csv(source.location)

    Map(raw_dbnsfp_annovar.id -> raw_dbnsfp_annovar.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(raw_dbnsfp_annovar.id)
      .select(
        $"#Chr" as "chromosome",
        $"Start".cast("long") as "start",
        $"End".cast("long") as "end",
        $"Ref" as "reference",
        $"Alt" as "alternate",
        $"DamagePredCount",
        cast("SIFT_score"),
        cast("SIFT_converted_rankscore"),
        $"SIFT_pred",
        cast("SIFT4G_score"),
        cast("SIFT4G_converted_rankscore"),
        $"SIFT4G_pred",
        cast("Polyphen2_HDIV_score"),
        cast("Polyphen2_HDIV_rankscore"),
        $"Polyphen2_HDIV_pred",
        cast("Polyphen2_HVAR_score"),
        cast("Polyphen2_HVAR_rankscore"),
        $"Polyphen2_HVAR_pred",
        cast("LRT_score"),
        cast("LRT_converted_rankscore"),
        $"LRT_pred",
        cast("MutationTaster_score"),
        cast("MutationTaster_converted_rankscore"),
        $"MutationTaster_pred",
        $"MutationAssessor_pred",
        cast("FATHMM_score"),
        cast("FATHMM_converted_rankscore"),
        $"FATHMM_pred",
        cast("PROVEAN_score"),
        cast("PROVEAN_converted_rankscore"),
        $"PROVEAN_pred",
        cast("VEST4_score"),
        cast("VEST4_rankscore"),
        cast("MetaSVM_score"),
        cast("MetaSVM_rankscore"),
        $"MetaSVM_pred",
        cast("MetaLR_score"),
        cast("MetaLR_rankscore"),
        $"MetaLR_pred",
        cast("MetaRNN_score"),
        cast("MetaRNN_rankscore"),
        $"MetaRNN_pred",
        cast("M-CAP_score"),
        cast("M-CAP_rankscore"),
        $"M-CAP_pred",
        cast("REVEL_score"),
        cast("REVEL_rankscore"),
        cast("MutPred_score"),
        cast("MutPred_rankscore"),
        cast("MVP_score"),
        cast("MVP_rankscore"),
        cast("MPC_score"),
        cast("MPC_rankscore"),
        cast("PrimateAI_score"),
        cast("PrimateAI_rankscore"),
        $"PrimateAI_pred",
        cast("DEOGEN2_score"),
        cast("DEOGEN2_rankscore"),
        $"DEOGEN2_pred",
        cast("BayesDel_addAF_score"),
        cast("BayesDel_addAF_rankscore"),
        $"BayesDel_addAF_pred",
        cast("BayesDel_noAF_score"),
        cast("BayesDel_noAF_rankscore"),
        $"BayesDel_noAF_pred",
        cast("ClinPred_score"),
        cast("ClinPred_rankscore"),
        $"ClinPred_pred",
        cast("LIST-S2_score"),
        cast("LIST-S2_rankscore"),
        $"LIST-S2_pred",
        $"Aloft_pred",
        $"Aloft_Confidence",
        cast("CADD_raw"),
        cast("CADD_raw_rankscore"),
        $"CADD_phred",
        cast("DANN_score"),
        cast("DANN_rankscore"),
        cast("fathmm-MKL_coding_score"),
        cast("fathmm-MKL_coding_rankscore"),
        $"fathmm-MKL_coding_pred",
        cast("fathmm-XF_coding_score"),
        cast("fathmm-XF_coding_rankscore"),
        $"fathmm-XF_coding_pred",
        cast("Eigen-raw_coding"),
        cast("Eigen-raw_coding_rankscore"),
        cast("Eigen-PC-raw_coding"),
        cast("Eigen-PC-raw_coding_rankscore"),
        cast("GenoCanyon_score"),
        cast("GenoCanyon_rankscore"),
        cast("integrated_fitCons_score"),
        cast("integrated_fitCons_rankscore"),
        $"integrated_confidence_value",

        cast("LINSIGHT"),
        cast("LINSIGHT_rankscore"),
        cast("GERP++_NR"),
        cast("GERP++_RS"),
        cast("GERP++_RS_rankscore"),
        cast("phyloP100way_vertebrate"),
        cast("phyloP100way_vertebrate_rankscore"),
        cast("phyloP30way_mammalian"),
        cast("phyloP30way_mammalian_rankscore"),
        cast("phastCons100way_vertebrate"),
        cast("phastCons100way_vertebrate_rankscore"),
        cast("phastCons30way_mammalian"),
        cast("phastCons30way_mammalian_rankscore"),
        cast("SiPhy_29way_logOdds"),
        cast("SiPhy_29way_logOdds_rankscore"),
        $"Interpro_domain",
        split($"GTEx_V8_gene", ";") as "GTEx_V8_gene",
        split($"GTEx_V8_tissue", ";") as "GTEx_V8_tissue",
        $"TWINSUK_AC".cast(LongType) as "TWINSUK_AC",
        cast("TWINSUK_AF"),
        $"ALSPAC_AC".cast(LongType) as "ALSPAC_AC",
        cast("ALSPAC_AF"),
        $"UK10K_AC".cast(LongType) as "UK10K_AC",
        cast("UK10K_AF"),
      )
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chomosome", "start"), n = Some(40))

}
