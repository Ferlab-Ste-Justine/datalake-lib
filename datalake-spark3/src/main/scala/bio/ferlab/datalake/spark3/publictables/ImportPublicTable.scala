package bio.ferlab.datalake.spark3.publictables

import bio.ferlab.datalake.spark3.SparkApp
import bio.ferlab.datalake.spark3.publictables.enriched.{DBNSFP, Genes}
import bio.ferlab.datalake.spark3.publictables.normalized.omim.OmimGeneSet
import bio.ferlab.datalake.spark3.publictables.normalized.orphanet.OrphanetGeneSet
import bio.ferlab.datalake.spark3.publictables.normalized.refseq.{RefSeqAnnotation, RefSeqHumanGenes}
import bio.ferlab.datalake.spark3.publictables.normalized._

object ImportPublicTable extends SparkApp {

  implicit val (conf, runSteps, spark) = init()

  val Array(_, _, tableName) = args

  tableName match {
    case "annovar_scores" => new AnnovarScores().run(runSteps)
    case "clinvar" => new Clinvar().run(runSteps)
    case "dbnsfp_raw" => new DBNSFPRaw().run(runSteps)
    case "dbnsfp" => new DBNSFP().run(runSteps)
    case "dbsnp" => new DBSNP().run(runSteps)
    case "ddd" => new DDDGeneSet().run(runSteps)
    case "ensembl_mapping" => new EnsemblMapping().run(runSteps)
    case "gnomadv3" => new GnomadV3().run(runSteps)
    case "genes" => new Genes().run(runSteps)
    case "hpo" => new HPOGeneSet().run(runSteps)
    case "omim" => new OmimGeneSet().run(runSteps)
    case "1000genomes" => new OneThousandGenomes().run(runSteps)
    case "orphanet" => new OrphanetGeneSet().run(runSteps)
    case "refseq_annotation" => new RefSeqAnnotation().run(runSteps)
    case "refseq_human_genes" => new RefSeqHumanGenes().run(runSteps)
    case "cosmic_gene_set" => new CosmicGeneSet().run(runSteps)
    case "topmed_bravo" => new TopMed().run(runSteps)
    case "spliceai_indel" => new normalized.SpliceAi(variantType = "indel").run(runSteps)
    case "spliceai_snv" => new normalized.SpliceAi(variantType = "snv").run(runSteps)
    case "spliceai_enriched" => new enriched.SpliceAi().run(runSteps)
  }
}
