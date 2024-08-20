package bio.ferlab.datalake.spark3.publictables

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.publictables.enriched.{DBNSFP, Genes, RareVariant}
import bio.ferlab.datalake.spark3.publictables.normalized._
import bio.ferlab.datalake.spark3.publictables.normalized.cosmic.{CosmicGeneSet, CosmicMutationSet}
import bio.ferlab.datalake.spark3.publictables.normalized.gnomad._
import bio.ferlab.datalake.spark3.publictables.normalized.omim.OmimGeneSet
import bio.ferlab.datalake.spark3.publictables.normalized.orphanet.OrphanetGeneSet
import bio.ferlab.datalake.spark3.publictables.normalized.refseq.{RefSeqAnnotation, RefSeqHumanGenes}
import mainargs.{ParserForMethods, main}

object ImportPublicTable {

  @main
  def annovar_scores(rc: RuntimeETLContext): Unit = AnnovarScores.run(rc)

  @main
  def clinvar(rc: RuntimeETLContext): Unit = Clinvar.run(rc)

  @main
  def cosmic_gene_set(rc: RuntimeETLContext): Unit = CosmicGeneSet.run(rc)

  @main
  def cosmic_mutation_set(rc: RuntimeETLContext): Unit = CosmicMutationSet.run(rc)

  @main
  def dbnsfp_raw(rc: RuntimeETLContext): Unit = DBNSFPRaw.run(rc)

  @main
  def dbnsfp(rc: RuntimeETLContext): Unit = DBNSFP.run(rc)

  @main
  def dbsnp(rc: RuntimeETLContext): Unit = DBSNP.run(rc)

  @main
  def ddd(rc: RuntimeETLContext): Unit = DDDGeneSet.run(rc)

  @main
  def ensembl_mapping(rc: RuntimeETLContext): Unit = EnsemblMapping.run(rc)

  @main
  def gnomadv3(rc: RuntimeETLContext): Unit = GnomadV3.run(rc)

  @main
  def gnomad_constraint(rc: RuntimeETLContext): Unit = GnomadConstraint.run(rc)

  @main
  def genes(rc: RuntimeETLContext): Unit = Genes.run(rc)

  @main
  def hpo(rc: RuntimeETLContext): Unit = HPOGeneSet.run(rc)

  @main
  def omim(rc: RuntimeETLContext): Unit = OmimGeneSet.run(rc)

  @main(name = "1000genomes")
  def one_thousand_genomes(rc: RuntimeETLContext): Unit = OneThousandGenomes.run(rc)

  @main
  def orphanet(rc: RuntimeETLContext): Unit = OrphanetGeneSet.run(rc)

  @main
  def refseq_annotation(rc: RuntimeETLContext): Unit = RefSeqAnnotation.run(rc)

  @main
  def refseq_human_genes(rc: RuntimeETLContext): Unit = RefSeqHumanGenes.run(rc)

  @main
  def spliceai_indel(rc: RuntimeETLContext): Unit = SpliceAi.run(rc, "indel")

  @main
  def spliceai_snv(rc: RuntimeETLContext): Unit = SpliceAi.run(rc, "snv")

  @main
  def spliceai_enriched_indel(rc: RuntimeETLContext): Unit = enriched.SpliceAi.run(rc, "indel")

  @main
  def spliceai_enriched_snv(rc: RuntimeETLContext): Unit = enriched.SpliceAi.run(rc, "snv")

  @main
  def topmed_bravo(rc: RuntimeETLContext): Unit = TopMed.run(rc)

  @main
  def rare_variant_enriched(rc: RuntimeETLContext): Unit = RareVariant.run(rc)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}


