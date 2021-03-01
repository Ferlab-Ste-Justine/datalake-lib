package ca.ferlab.datalake.core.config

case class StorageConf(alias: String, path: String)

case class Configuration(storages: List[StorageConf])
