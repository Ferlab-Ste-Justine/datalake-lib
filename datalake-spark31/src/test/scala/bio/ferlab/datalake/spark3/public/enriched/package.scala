package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.commons.config.ConfigurationLoader
import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, StorageConf}

package object enriched {

  val alias = "public_database"

  implicit val conf: Configuration =
    ConfigurationLoader.loadFromResources("config/reference_kf.conf")
      .copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile)))

}
