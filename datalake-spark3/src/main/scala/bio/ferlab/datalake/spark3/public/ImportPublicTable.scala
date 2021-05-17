package bio.ferlab.datalake.spark3.public

object ImportPublicTable extends SparkApp {

  val Array(_, tableName) = args

  tableName match {
    case "genes" => new ImportGenesTable().run()
  }
}
