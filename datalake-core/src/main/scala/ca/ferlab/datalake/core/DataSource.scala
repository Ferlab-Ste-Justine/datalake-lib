package ca.ferlab.datalake.core

import ca.ferlab.datalake.core.config.Configuration

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
