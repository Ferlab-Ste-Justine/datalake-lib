package bio.ferlab.datalake.spark3.config

/**
 * Base configuration needed for an ETL job
 * @param storages list of storages associated with aliases
 * @param sources list of data sources
 * @param args arguments passed to the job
 * @param sparkconf extra configuration for the spark conf
 */
case class Configuration(storages: List[StorageConf] = List(),
                         sources: List[DatasetConf] = List(),
                         args: List[String] = List.empty[String],
                         sparkconf: Map[String, String] = Map()) {

  def getDataset(database: String, name: String): DatasetConf =
    sources.find(_.table.contains(TableConf(database, name)))
      .getOrElse(throw new Exception(s"""Dataset [$database.$name] not found in ${sources.map(_.table).mkString("[", ", ", "]")}."""))

  def getDataset(id: String): DatasetConf =
    sources.find(_.datasetid.equalsIgnoreCase(id))
      .getOrElse(throw new Exception(s"""Dataset [$id] not found in ${sources.map(_.datasetid).mkString("[", ", ", "]")}."""))

  def getStorage(alias: String): String = {
    storages.find(_.storageid.equalsIgnoreCase(alias))
      .map(_.path)
      .getOrElse(throw new IllegalArgumentException(s"storage with alias [$alias] not found"))
  }

  def +(that: Configuration): Configuration =
    Configuration(
      this.storages ++ that.storages,
      this.sources ++ that.sources,
      this.args ++ that.args,
      this.sparkconf ++ that.sparkconf,
    )
}

object Configuration {
  def empty: Configuration = Configuration()
}
