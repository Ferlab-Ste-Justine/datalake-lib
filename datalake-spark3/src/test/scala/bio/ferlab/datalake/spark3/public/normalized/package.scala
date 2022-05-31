package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.commons.config.ConfigurationLoader
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3

package object normalized {

  val alias = "public_database"

  implicit val conf: Configuration =
    ConfigurationLoader.loadFromResources("config/reference_kf.conf")
      .copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile, S3)))

}
