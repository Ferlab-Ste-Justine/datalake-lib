package bio.ferlab.datalake.spark3.publictables.normalized.cosmic

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.transformation.Cast.castInt
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{concat, lit, regexp_extract, split}
import org.apache.spark.sql.types.LongType

import java.time.LocalDateTime

case class CosmicMutationSet(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  private val cosmic_mutation_set = conf.getDataset("raw_cosmic_mutation_set")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(cosmic_mutation_set.id -> cosmic_mutation_set.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime): DataFrame = {
    import spark.implicits._

    data(cosmic_mutation_set.id)
      .select(
        regexp_extract($"Mutation genome position GRCh38", "(.+):(\\d+)-(\\d+)", 1) as "chromosome",
        regexp_extract($"Mutation genome position GRCh38", "(.+):(\\d+)-(\\d+)", 2).cast(LongType) as "start",
        regexp_extract($"Mutation genome position GRCh38", "(.+):(\\d+)-(\\d+)", 3).cast(LongType) as "end",
        $"GENOMIC_WT_ALLELE_SEQ" as "reference",
        $"GENOMIC_MUT_ALLELE_SEQ" as "alternate",
        concat($"MUTATION_URL", lit("&genome=37")) as "mutation_url",
        castInt("SHARED_AA") as "shared_aa",
        $"GENOMIC_MUTATION_ID" as "genomic_mutation_id",
        castInt("COSMIC_SAMPLE_MUTATED") as "cosmic_sample_mutated",
        castInt("COSMIC_SAMPLE_TESTED") as "cosmic_sample_tested",
        split($"DNDS_DISEASE_QVAL_SIG", ";") as "dnds_disease_qval_sig",
        $"MUTATION_SIGNIFICANCE_TIER" as "mutation_significance_tier", // Not casted to int since possible values are : 1, 2, 3, Other
        split($"ONC_TSG", ",") as "onc_tsg",
        castInt("CGC_TIER") as "cgc_tier",
        $"GENE_NAME" as "gene_name",
        $"ACCESSION_NUMBER" as "accession_number",
        $"LEGACY_MUTATION_ID" as "legacy_mutation_id",
        $"Mutation CDS" as "mutation_cds",
        $"Mutation AA" as "mutation_aa",
        split($"DISEASE", ";") as "disease",
        split($"WGS_DISEASE", ";") as "wgs_disease",
        castInt("AA_MUT_START") as "aa_mut_start",
        castInt("AA_MUT_STOP") as "aa_mut_stop",
        $"AA_WT_ALLELE_SEQ" as "aa_wt_allele_seq",
        $"AA_MUT_ALLELE_SEQ" as "aa_mut_allele_seq",
        $"Mutation Description CDS" as "mutation_description_cds",
        $"Mutation Description AA" as "mutation_description_aa",
        $"ONTOLOGY_MUTATION_CODE" as "ontology_mutation_code"
      )
  }
}

object CosmicMutationSet {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    CosmicMutationSet(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
