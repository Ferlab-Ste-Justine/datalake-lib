package bio.ferlab.datalake.commons.config

abstract class ConfigurationWrapper(datalake: DatalakeConf) extends Configuration {

  def storages: List[StorageConf] = datalake.storages

  def sources: List[DatasetConf] = datalake.sources

  def args: List[String] = datalake.args

  def sparkconf: Map[String, String] = datalake.sparkconf

  /**
   * Concatenate this Configuration with that Configuration.
   *
   * @param that the other Configuration object to concatenate with.
   * @return a single Configuration object.
   */
  def +(that: Configuration): Configuration =
    SimpleConfiguration(DatalakeConf(
      this.storages ++ that.storages,
      this.sources ++ that.sources,
      this.args ++ that.args,
      this.sparkconf ++ that.sparkconf,
    ))

}
