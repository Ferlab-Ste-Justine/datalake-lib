package bio.ferlab.datalake.commons.config

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

  /**
   * Fetch a dataset based on its database and table name.
   * @param database database name
   * @param name table name
   * @return a DatasetConf or throw an exception if the dataset does not exists.
   */
  def getDataset(database: String, name: String): DatasetConf =
    sources.find(_.table.contains(TableConf(database, name)))
      .getOrElse(throw new Exception(s"""Dataset [$database.$name] not found in ${sources.map(_.table).mkString("[", ", ", "]")}."""))

  /**
   * Fetch a dataset based on its unique id.
   * @param id dataset unique id
   * @return a DatasetConf or throw an exception if the dataset does not exists.
   */
  def getDataset(id: String): DatasetConf =
    sources.find(_.id.equalsIgnoreCase(id))
      .getOrElse(throw new Exception(s"""Dataset [$id] not found in ${sources.map(_.id).mkString("[", ", ", "]")}."""))

  /**
   * Fetch a storage based on its alias.
   * @param alias storage alias
   * @return a StorageConf or throw an exception if the dataset does not exists.
   */
  def getStorage(alias: String): StorageConf = {
    storages.find(_.id.equalsIgnoreCase(alias))
      .getOrElse(throw new IllegalArgumentException(s"storage with alias [$alias] not found"))
  }

  /**
   * Concatenate this Configuration with that Configuration.
   * @param that the other Configuration object to concatenate with.
   * @return a single Configuration object.
   */
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
