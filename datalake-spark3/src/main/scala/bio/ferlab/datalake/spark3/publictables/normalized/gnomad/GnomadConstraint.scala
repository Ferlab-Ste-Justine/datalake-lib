package bio.ferlab.datalake.spark3.publictables.normalized.gnomad

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.transformation.Cast.{castDouble, castFloat, castInt, castLong}
import bio.ferlab.datalake.spark3.utils.RepartitionByRange
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class GnomadConstraint(implicit conf: Configuration) extends ETLP {
  override val mainDestination: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")
  val raw_gnomad_constraint: DatasetConf = conf.getDataset("raw_gnomad_constraint_v2_1_1")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())
                      (implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(raw_gnomad_constraint.id -> raw_gnomad_constraint.read)
  }

    override def transformSingle(data: Map[String, DataFrame],
                                 lastRunDateTime: LocalDateTime = minDateTime,
                                 currentRunDateTime: LocalDateTime = LocalDateTime.now())
                                (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    data(raw_gnomad_constraint.id)
      .select(
        $"chromosome",
        castLong("start_position") as "start" ,
        castLong("end_position") as "end",
        $"gene" as "symbol",
        $"transcript",
        castInt("obs_mis"),
        castFloat("exp_mis"),
        castFloat("oe_mis"),
        castFloat("mu_mis"),
        castLong("possible_mis"),
        castInt("obs_mis_pphen"),
        castFloat("exp_mis_pphen"),
        castFloat("oe_mis_pphen"),
        castLong("possible_mis_pphen"),
        castInt("obs_syn"),
        castFloat("exp_syn"),
        castFloat("oe_syn"),
        castFloat("mu_syn"),
        castLong("possible_syn"),
        castInt("obs_lof"),
        castFloat("mu_lof"),
        castInt("possible_lof"),
        castFloat("exp_lof"),
        castFloat("pLI"),
        castDouble("pNull"),
        castDouble("pRec"),
        castFloat("oe_lof"),
        castFloat("oe_syn_lower"),
        castFloat("oe_syn_upper"),
        castFloat("oe_mis_lower"),
        castFloat("oe_mis_upper"),
        castFloat("oe_lof_lower"),
        castFloat("oe_lof_upper"),
        $"constraint_flag",
        castFloat("syn_z"),
        castFloat("mis_z"),
        castFloat("lof_z"),
        castInt("oe_lof_upper_rank"),
        castInt("oe_lof_upper_bin"),
        castInt("oe_lof_upper_bin_6"),
        castInt("n_sites"),
        castFloat("classic_caf"),
        castFloat("max_af"),
        castLong("no_lofs"),
        castInt("obs_het_lof"),
        castInt("obs_hom_lof"),
        castLong("defined"),
        castFloat("p"),
        castFloat("exp_hom_lof"),
        castFloat("classic_caf_afr"),
        castFloat("classic_caf_amr"),
        castFloat("classic_caf_asj"),
        castFloat("classic_caf_eas"),
        castFloat("classic_caf_fin"),
        castFloat("classic_caf_nfe"),
        castFloat("classic_caf_oth"),
        castFloat("classic_caf_sas"),
        castFloat("p_afr"),
        castFloat("p_amr"),
        castFloat("p_asj"),
        castFloat("p_eas"),
        castFloat("p_fin"),
        castFloat("p_nfe"),
        castFloat("p_oth"),
        castFloat("p_sas"),
        $"transcript_type",
        $"gene_id",
        castInt("transcript_level"),
        castLong("cds_length"),
        castInt("num_coding_exons"),
        $"gene_type",
        castLong("gene_length"),
        castFloat("exac_pLI"),
        castInt("exac_obs_lof"),
        castFloat("exac_exp_lof"),
        castFloat("exac_oe_lof"),
        $"brain_expression"
      )
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1))

}
