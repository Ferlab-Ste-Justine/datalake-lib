package bio.ferlab.datalake.spark3.config

/**
 * Configuration of a storage endpoint
 * @param alias unique identifier to the storage. should match alias given to a [[SourceConf]]
 * @param path path to the storage
 */
case class StorageConf(alias: String, path: String)
