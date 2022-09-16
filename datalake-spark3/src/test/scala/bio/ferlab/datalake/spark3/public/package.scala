package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import pureconfig.generic.auto._
import pureconfig.module.enum._
package object public {

  val alias = "kf-strides-variant"

  private val sc: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/reference_kf.conf")
  implicit val conf: Configuration =
    sc
      .copy(datalake = sc.datalake.copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile, S3))))

}
