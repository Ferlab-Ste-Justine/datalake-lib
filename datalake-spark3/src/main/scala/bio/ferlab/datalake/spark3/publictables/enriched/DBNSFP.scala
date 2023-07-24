package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns}
import bio.ferlab.datalake.spark3.etl.RuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.transformation.Cast.{castDouble, castLong}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDateTime

case class DBNSFP(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {
override val mainDestination: DatasetConf = conf.getDataset("enriched_dbnsfp")
  val normalized_dbnsfp: DatasetConf = conf.getDataset("normalized_dbnsfp")

  def split_semicolon(colName: String, outputColName: String): Column =
    split(col(colName), ";") as outputColName

  def split_semicolon(colName: String): Column = split_semicolon(colName, colName)

  def element_at_postion(colName: String): Column =
    element_at(col(colName), col("position")) as colName

  def score(colName: String): Column = when(element_at_postion(colName) === ".", null)
    .otherwise(element_at_postion(colName).cast(DoubleType)) as colName

  def pred(colName: String): Column = when(element_at_postion(colName) === ".", null)
    .otherwise(element_at_postion(colName)) as colName

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      normalized_dbnsfp.id-> normalized_dbnsfp.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(normalized_dbnsfp.id)
      .select(
        col("chromosome"),
        castLong("start"),
        col("reference"),
        col("alternate"),
        split_semicolon("aapos"),
        split_semicolon("refcodon"),
        split_semicolon("codonpos"),
        col("aaref"),
        split_semicolon("Ensembl_proteinid", "ensembl_protein_id"),
        split_semicolon("genename", "symbol"),
        split_semicolon("Ensembl_geneid", "ensembl_gene_id"),
        split_semicolon("VEP_canonical"),
        posexplode(split(col("Ensembl_transcriptid"), ";")),
        split_semicolon("cds_strand"),
        split_semicolon("SIFT_score"),
        split_semicolon("SIFT_pred"),
        col("SIFT_converted_rankscore"),
        split_semicolon("Polyphen2_HDIV_score"),
        split_semicolon("Polyphen2_HDIV_pred"),
        col("Polyphen2_HDIV_rankscore"),
        split_semicolon("Polyphen2_HVAR_score"),
        split_semicolon("Polyphen2_HVAR_pred"),
        col("Polyphen2_HVAR_rankscore"),
        split_semicolon("FATHMM_score"),
        split_semicolon("FATHMM_pred"),
        col("FATHMM_converted_rankscore"),
        col("REVEL_rankscore"),
        split_semicolon("REVEL_score"),
        col("LRT_converted_rankscore"),
        col("LRT_pred"),
        col("LRT_score"),
        col("CADD_raw"),
        col("CADD_raw_rankscore"),
        col("CADD_phred"),
        col("DANN_score"),
        col("DANN_rankscore"),
        col("phyloP100way_vertebrate"),
        col("phyloP100way_vertebrate_rankscore"),
        col("phyloP30way_mammalian"),
        col("phyloP30way_mammalian_rankscore"),
        col("phyloP17way_primate"),
        col("phyloP17way_primate_rankscore"),
        col("phastCons100way_vertebrate"),
        col("phastCons100way_vertebrate_rankscore"),
        col("phastCons30way_mammalian"),
        col("phastCons30way_mammalian_rankscore"),
        col("phastCons17way_primate"),
        col("phastCons17way_primate_rankscore"),
        col("GERP++_NR"),
        col("GERP++_RS"),
        col("GERP++_RS_rankscore"),
        col("MutPred_rankscore"),
        col("MutPred_score"),
        split_semicolon("MutationAssessor_pred"),
        split_semicolon("MutationAssessor_score"),
        col("MutationAssessor_rankscore"),
        col("MutationTaster_converted_rankscore"),
        split_semicolon("PROVEAN_pred"),
        split_semicolon("PROVEAN_score"),
        col("PROVEAN_converted_rankscore"),
        split_semicolon("VEST4_score"),
        col("VEST4_rankscore"),
        col("MetaSVM_pred"),
        col("MetaSVM_rankscore"),
        col("MetaSVM_score"),
        col("MetaLR_pred"),
        col("MetaLR_rankscore"),
        col("MetaLR_score"),
        col("Reliability_index"),
        col("MetaRNN_pred"),
        col("MetaRNN_rankscore"),
        col("MetaRNN_score"),
        col("M-CAP_pred"),
        col("M-CAP_rankscore"),
        col("M-CAP_score"),
        split_semicolon("MPC_score"),
        col("MPC_rankscore"),
        split_semicolon("MVP_score"),
        col("MVP_rankscore"),
        col("PrimateAI_pred"),
        col("PrimateAI_rankscore"),
        col("PrimateAI_score"),
        split_semicolon("DEOGEN2_pred"),
        split_semicolon("DEOGEN2_score"),
        col("DEOGEN2_rankscore"),
        col("BayesDel_addAF_pred"),
        col("BayesDel_addAF_rankscore"),
        col("BayesDel_addAF_score"),
        col("BayesDel_noAF_pred"),
        col("BayesDel_noAF_rankscore"),
        col("BayesDel_noAF_score"),
        col("ClinPred_pred"),
        col("ClinPred_rankscore"),
        col("ClinPred_score"),
        split_semicolon("LIST-S2_pred"),
        split_semicolon("LIST-S2_score"),
        col("LIST-S2_rankscore"),
        col("fathmm-MKL_coding_pred"),
        col("fathmm-MKL_coding_rankscore"),
        col("fathmm-MKL_coding_score"),
        col("fathmm-MKL_coding_group"),
        col("fathmm-XF_coding_pred"),
        col("fathmm-XF_coding_rankscore"),
        col("fathmm-XF_coding_score"),
        col("Eigen-PC-phred_coding"),
        col("Eigen-PC-raw_coding"),
        col("Eigen-PC-raw_coding_rankscore"),
        col("Eigen-phred_coding"),
        col("Eigen-raw_coding"),
        col("Eigen-raw_coding_rankscore"),
        col("GenoCanyon_rankscore"),
        col("GenoCanyon_score"),
        col("integrated_confidence_value"),
        col("integrated_fitCons_rankscore"),
        col("integrated_fitCons_score"),
        col("GM12878_confidence_value"),
        col("GM12878_fitCons_rankscore"),
        col("GM12878_fitCons_score"),
        col("H1-hESC_confidence_value"),
        col("H1-hESC_fitCons_rankscore"),
        col("H1-hESC_fitCons_score"),
        col("HUVEC_confidence_value"),
        col("HUVEC_fitCons_rankscore"),
        col("HUVEC_fitCons_score"),
        col("LINSIGHT"),
        col("LINSIGHT_rankscore"),
        col("bStatistic"),
        col("bStatistic_converted_rankscore"),
        split_semicolon("Interpro_domain"),
        split_semicolon("GTEx_V8_gene"),
        split_semicolon("GTEx_V8_tissue")
      )
      .withColumnRenamed("col", "ensembl_transcript_id")
      .withColumn("position", col("pos") + 1)
      .drop("pos")
      .select(
        col("chromosome"),
        col("start"),
        col("reference"),
        col("alternate"),
        col("aaref"),
        element_at_postion("symbol"),
        element_at_postion("ensembl_gene_id"),
        element_at_postion("ensembl_protein_id"),
        element_at_postion("VEP_canonical"),
        col("ensembl_transcript_id"),
        element_at_postion("cds_strand").cast(IntegerType),
        score("SIFT_score"),
        pred("SIFT_pred"),
        castDouble("SIFT_converted_rankscore"),
        score("Polyphen2_HDIV_score"),
        pred("Polyphen2_HDIV_pred"),
        castDouble("Polyphen2_HDIV_rankscore"),
        score("Polyphen2_HVAR_score"),
        pred("Polyphen2_HVAR_pred"),
        castDouble("Polyphen2_HVAR_rankscore"),
        score("FATHMM_score"),
        pred("FATHMM_pred"),
        castDouble("FATHMM_converted_rankscore"),
        castDouble("CADD_raw"),
        castDouble("CADD_raw_rankscore"),
        castDouble("CADD_phred"),
        castDouble("DANN_score"),
        castDouble("DANN_rankscore"),
        castDouble("REVEL_rankscore"),
        score("REVEL_score"),
        castDouble("LRT_converted_rankscore"),
        col("LRT_pred"),
        castDouble("LRT_score"),
        castDouble("phyloP100way_vertebrate"),
        castDouble("phyloP100way_vertebrate_rankscore"),
        castDouble("phyloP30way_mammalian"),
        castDouble("phyloP30way_mammalian_rankscore"),
        castDouble("phyloP17way_primate"),
        castDouble("phyloP17way_primate_rankscore"),
        castDouble("phastCons100way_vertebrate"),
        castDouble("phastCons100way_vertebrate_rankscore"),
        castDouble("phastcons30way_mammalian"),
        castDouble("phastCons30way_mammalian_rankscore"),
        castDouble("phastCons17way_primate"),
        castDouble("phastCons17way_primate_rankscore"),
        castDouble("GERP++_NR"),
        castDouble("GERP++_RS"),
        castDouble("GERP++_RS_rankscore"),
        castDouble("MutPred_rankscore"),
        castDouble("MutPred_score"),
        pred("MutationAssessor_pred"),
        score("MutationAssessor_score"),
        castDouble("MutationAssessor_rankscore"),
        castDouble("MutationTaster_converted_rankscore"),
        pred("PROVEAN_pred"),
        score("PROVEAN_score"),
        castDouble("PROVEAN_converted_rankscore"),
        score("VEST4_score"),
        castDouble("VEST4_rankscore"),
        col("MetaSVM_pred"),
        castDouble("MetaSVM_rankscore"),
        castDouble("MetaSVM_score"),
        col("MetaLR_pred"),
        castDouble("MetaLR_rankscore"),
        castDouble("MetaLR_score"),
        castDouble("Reliability_index"),
        col("MetaRNN_pred"),
        castDouble("MetaRNN_rankscore"),
        castDouble("MetaRNN_score"),
        col("M-CAP_pred"),
        castDouble("M-CAP_score"),
        castDouble("M-CAP_rankscore"),
        score("MPC_score"),
        castDouble("MPC_rankscore"),
        score("MVP_score"),
        castDouble("MVP_rankscore"),
        col("PrimateAI_pred"),
        castDouble("PrimateAI_rankscore"),
        castDouble("PrimateAI_score"),
        pred("DEOGEN2_pred"),
        score("DEOGEN2_score"),
        castDouble("DEOGEN2_rankscore"),
        col("BayesDel_addAF_pred"),
        castDouble("BayesDel_addAF_rankscore"),
        castDouble("BayesDel_addAF_score"),
        col("BayesDel_noAF_pred"),
        castDouble("BayesDel_noAF_rankscore"),
        castDouble("BayesDel_noAF_score"),
        col("ClinPred_pred"),
        castDouble("ClinPred_rankscore"),
        castDouble("ClinPred_score"),
        pred("LIST-S2_pred"),
        score("LIST-S2_score"),
        castDouble("LIST-S2_rankscore"),
        col("fathmm-MKL_coding_pred"),
        castDouble("fathmm-MKL_coding_rankscore"),
        castDouble("fathmm-MKL_coding_score"),
        col("fathmm-MKL_coding_group"),
        col("fathmm-XF_coding_pred"),
        castDouble("fathmm-XF_coding_rankscore"),
        castDouble("fathmm-XF_coding_score"),
        castDouble("Eigen-PC-phred_coding"),
        castDouble("Eigen-PC-raw_coding"),
        castDouble("Eigen-PC-raw_coding_rankscore"),
        castDouble("Eigen-phred_coding"),
        castDouble("Eigen-raw_coding"),
        castDouble("Eigen-raw_coding_rankscore"),
        castDouble("GenoCanyon_rankscore"),
        castDouble("GenoCanyon_score"),
        castDouble("integrated_confidence_value"),
        castDouble("integrated_fitCons_rankscore"),
        castDouble("integrated_fitCons_score"),
        castDouble("GM12878_confidence_value"),
        castDouble("GM12878_fitCons_rankscore"),
        castDouble("GM12878_fitCons_score"),
        castDouble("H1-hESC_confidence_value"),
        castDouble("H1-hESC_fitCons_rankscore"),
        castDouble("H1-hESC_fitCons_score"),
        castDouble("HUVEC_confidence_value"),
        castDouble("HUVEC_fitCons_rankscore"),
        castDouble("HUVEC_fitCons_score"),
        castDouble("LINSIGHT"),
        castDouble("LINSIGHT_rankscore"),
        castDouble("bStatistic"),
        castDouble("bStatistic_converted_rankscore"),
        element_at_postion("Interpro_domain"),
        col("GTEx_V8_gene"),
        col("GTEx_V8_tissue")
      )
      .drop("position")

  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq("start"))

}

object DBNSFP {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    DBNSFP(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}