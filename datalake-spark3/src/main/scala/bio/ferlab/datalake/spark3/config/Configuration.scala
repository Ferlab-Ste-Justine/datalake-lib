package bio.ferlab.datalake.spark3.config

/**
 * Base configuration needed for an ETL job
 * @param storages list of storages associated with aliases
 * @param sources list of data sources
 * @param args arguments passed to the job
 * @param sparkconf extra configuration for the spark conf
 */
case class Configuration(storages: List[StorageConf] = List(),
                         sources: List[SourceConf] = List(),
                         args: List[String] = List.empty[String],
                         sparkconf: Map[String, String] = Map()) {

  def getSource(database: String, name: String): SourceConf =
    sources
      .find(s => s.name.equalsIgnoreCase(name) && s.database.equalsIgnoreCase(database))
      .getOrElse(throw new Exception(s"""Source [$database.$name] not found in ${sources.map(s => s"${s.database}.${s.name}").mkString("[", ", ", "]")}."""))

  def getSource(name: String): SourceConf =
    sources.find(_.name.equalsIgnoreCase(name))
      .getOrElse(throw new Exception(s"""Source [$name] not found in ${sources.map(_.name).mkString("[", ", ", "]")}."""))

  def getStorage(alias: String): String = {
    storages.find(_.alias.equalsIgnoreCase(alias))
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
