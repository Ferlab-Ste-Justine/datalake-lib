package bio.ferlab.datalake.core.etl

import bio.ferlab.datalake.core.config.Configuration

/**
 * Abstraction on a data source
 * @param storageAlias an alias designating where the data is sitting.
 *                     this can point to an object store url in the configuration like s3://my-bucket/
 * @param relativePath the relative path from the root of the storage to the data source. ie, /raw/my-system/my-source
 * @param database name of the database where the data is
 * @param name name of the source inside the database. the combining of database and name should be unique
 * @param format the format of the data
 * @param readOptions OPTIONAL - read options to pass to spark in order to read the data into a DataFrame
 * @param writeOptions OPTIONAL - write options to pass to spark in order to write the data into files
 */
case class DataSource(storageAlias: String,
                      relativePath: String,
                      database: String,
                      name: String,
                      format: Format,
                      readOptions: Map[String, String] = Map.empty[String, String],
                      writeOptions: Map[String, String] = Map.empty[String, String]) {

  def location(implicit config: Configuration): String = {
    val rootPath: String = config.storages.find(_.alias.equalsIgnoreCase(storageAlias))
      .map(_.path)
      .getOrElse(throw new IllegalArgumentException(s"storage with alias [$storageAlias] not found"))

    s"$rootPath/$relativePath".replace("//", "/")
  }
}
