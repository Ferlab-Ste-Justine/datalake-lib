package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.commons.config.ConfigurationLoader
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3

package object public {

  val alias = "kf-strides-variant"

  implicit val conf: Configuration =
    ConfigurationLoader.loadFromResources("config/reference_kf.conf")
      .copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile, S3)))

}
