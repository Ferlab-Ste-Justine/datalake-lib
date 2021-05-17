package bio.ferlab.datalake.spark3.config

import bio.ferlab.datalake.spark3.loader.{Format, LoadType}

/**
 * Abstraction on a data source
 * @param alias an alias designating where the data is sitting.
 *                     this can point to an object store url in the configuration like s3://my-bucket/
 * @param path the relative path from the root of the storage to the data source. ie, /raw/my-system/my-source
 * @param database name of the database where the data is
 * @param name name of the source inside the database. the combining of database and name should be unique
 * @param format data format
 * @param loadtype how the data is written
 * @param readoptions OPTIONAL - read options to pass to spark in order to read the data into a DataFrame
 * @param writeoptions OPTIONAL - write options to pass to spark in order to write the data into files
 * @param documentationpath OPTIONAL - path to the documentation file
 * @param view OPTIONAL - schema of the view pointing to the concrete table
 */
case class SourceConf(alias: String,
                      path: String,
                      database: String,
                      name: String,
                      format: Format,
                      loadtype: LoadType,
                      keys: List[String] = List(),
                      partitionby: List[String] = List(),
                      readoptions: Map[String, String] = Map(),
                      writeoptions: Map[String, String] = Map(),
                      documentationpath: String = "",
                      view: String = "") {

  def rootPath(implicit config: Configuration): String = {
    config.getStorage(alias)
  }

  def location(implicit config: Configuration): String = {
    s"$rootPath$path"
  }
}
