package bio.ferlab.datalake.spark3.public.normalized

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class AnnovarScores()(implicit conf: Configuration) extends ETLP {

  override val destination: DatasetConf = conf.getDataset("normalized_dbnsfp_annovar")
  val raw_dbnsfp_annovar: DatasetConf = conf.getDataset("raw_dbnsfp_annovar")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(raw_dbnsfp_annovar.id -> raw_dbnsfp_annovar.read)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime,
                         currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(raw_dbnsfp_annovar.id)
      .select(
        $"#Chr" as "chromosome",
        $"Start".cast("long") as "start",
        $"End".cast("long") as "end",
        $"Ref" as "reference",
        $"Alt" as "alternate",
        $"DamagePredCount".cast("double") as "DamagePredCount",
        $"SIFT_pred",
        $"SIFT4G_pred",
        $"Polyphen2_HDIV_pred",
        $"Polyphen2_HVAR_pred",
        $"LRT_pred",
        $"MutationTaster_pred",
        $"MutationAssessor_pred",
        $"FATHMM_pred",
        $"PROVEAN_pred",
        $"VEST4_score".cast("double") as "VEST4_score",
        $"MetaSVM_pred",
        $"MetaLR_pred",
        $"M-CAP_pred",
        $"REVEL_score".cast("double") as "REVEL_score",
        $"MutPred_score".cast("double") as "MutPred_score",
        $"MVP_score".cast("double") as "MVP_score",
        $"MPC_score".cast("double") as "MPC_score",
        $"PrimateAI_pred",
        $"DEOGEN2_pred",
        $"BayesDel_addAF_pred",
        $"BayesDel_noAF_pred",
        $"ClinPred_pred",
        $"LIST-S2_pred",
        $"CADD_raw",
        $"CADD_phred",
        $"DANN_score".cast("double") as "DANN_score",
        $"fathmm-MKL_coding_pred",
        $"fathmm-XF_coding_pred",
        $"Eigen-raw_coding",
        $"Eigen-phred_coding",
        $"Eigen-PC-raw_coding",
        $"Eigen-PC-phred_coding",
        $"GenoCanyon_score".cast("double") as "GenoCanyon_score",
        $"integrated_fitCons_score".cast("double") as "integrated_fitCons_score",
        $"GM12878_fitCons_score".cast("double") as "GM12878_fitCons_score",
        $"H1-hESC_fitCons_score".cast("double") as "H1-hESC_fitCons_score",
        $"HUVEC_fitCons_score".cast("double") as "HUVEC_fitCons_score",
        $"LINSIGHT",
        $"GERP++_NR".cast("double") as "GERP++_NR",
        $"GERP++_RS".cast("double") as "GERP++_RS",
        $"phyloP100way_vertebrate".cast("double") as "phyloP100way_vertebrate",
        $"phyloP30way_mammalian".cast("double") as "phyloP30way_mammalian",
        $"phyloP17way_primate".cast("double") as "phyloP17way_primate",
        $"phastCons100way_vertebrate".cast("double") as "phastCons100way_vertebrate",
        $"phastCons30way_mammalian".cast("double") as "phastCons30way_mammalian",
        $"phastCons17way_primate".cast("double") as "phastCons17way_primate",
        $"bStatistic".cast("double") as "bStatistic",
        $"Interpro_domain",
        split($"GTEx_V8_gene", ";") as "GTEx_V8_gene",
        split($"GTEx_V8_tissue", ";") as "GTEx_V8_tissue"
      )
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime,
                    currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    super.load(data
      .repartition(col("chromosome"))
      .sortWithinPartitions("start"),
        lastRunDateTime,
        currentRunDateTime)
  }
}

