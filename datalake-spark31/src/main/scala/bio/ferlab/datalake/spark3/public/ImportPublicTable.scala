package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.public.enriched.Genes
import bio.ferlab.datalake.spark3.public.normalized.Clinvar

object ImportPublicTable extends SparkApp {

  implicit val (conf, runSteps, spark) = init()

  val Array(_, tableName) = args

  tableName match {
    case "clinvar" => new Clinvar().run(runSteps)
    case "genes" => new Genes().run(runSteps)
  }
}
