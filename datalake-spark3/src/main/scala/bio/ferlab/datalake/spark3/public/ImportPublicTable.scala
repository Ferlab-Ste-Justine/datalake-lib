package bio.ferlab.datalake.spark3.public

object ImportPublicTable extends SparkApp {

  implicit val (conf, spark) = init()

  val Array(_, tableName) = args

  tableName match {
    case "clinvar" => new ImportClinvar().run()
    case "genes" => new ImportGenesTable().run()
  }
}
