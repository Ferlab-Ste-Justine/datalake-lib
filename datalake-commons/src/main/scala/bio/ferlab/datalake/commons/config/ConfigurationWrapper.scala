package bio.ferlab.datalake.commons.config

abstract class ConfigurationWrapper(datalake: DatalakeConf) extends Configuration {

  def storages: List[StorageConf] = datalake.storages

  def sources: List[DatasetConf] = datalake.sources

  def args: List[String] = datalake.args

  def sparkconf: Map[String, String] = datalake.sparkconf

}
