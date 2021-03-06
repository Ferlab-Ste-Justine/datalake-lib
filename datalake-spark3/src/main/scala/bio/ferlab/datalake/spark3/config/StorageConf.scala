package bio.ferlab.datalake.spark3.config

/**
 * Configuration of a storage endpoint
 * @param id unique identifier to the storage. should match alias given to a [[DatasetConf]]
 * @param path path to the storage
 */
case class StorageConf(id: String, path: String)
