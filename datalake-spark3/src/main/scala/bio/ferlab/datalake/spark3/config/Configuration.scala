package bio.ferlab.datalake.spark3.config

case class StorageConf(alias: String, path: String)

case class Configuration(storages: List[StorageConf])
