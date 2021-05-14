package bio.ferlab.datalake.spark3.public

object ImportPublicTable extends SparkApp {

  val tableName = args(1)

  tableName match {
    case "genes" => new ImportGenesTable().run()
  }
}
