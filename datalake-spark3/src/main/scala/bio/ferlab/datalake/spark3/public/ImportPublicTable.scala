package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.public.enriched.{DBNSFP, Genes}
import bio.ferlab.datalake.spark3.public.normalized.{Clinvar, DBNSFPRaw, EnsemblMapping, GnomadV3}
import org.kidsfirstdrc.dwh.external.dbnsfp.AnnovarScores

object ImportPublicTable extends SparkApp {

  implicit val (conf, runSteps, spark) = init()

  val Array(_, tableName) = args

  tableName match {
    case "clinvar" => new Clinvar().run(runSteps)
    case "gnomadv3" => new GnomadV3().run(runSteps)
    case "ensembl_mapping" => new EnsemblMapping().run(runSteps)
    case "genes" => new Genes().run(runSteps)
    case "dbnsfp_raw" => new DBNSFPRaw().run(runSteps)
    case "dbnsfp" => new DBNSFP().run(runSteps)
    case "annovar_scores" => new AnnovarScores().run(runSteps)
  }
}
