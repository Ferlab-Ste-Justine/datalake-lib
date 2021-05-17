package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationLoader, StorageConf}

package object public {

  val alias = "kf-strides-variant"

  implicit val conf: Configuration =
    ConfigurationLoader.loadFromResources("config/reference_kf.conf")
      .copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile)))

}
