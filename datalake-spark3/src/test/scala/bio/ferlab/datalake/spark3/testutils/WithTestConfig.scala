package bio.ferlab.datalake.spark3.testutils

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.{LOCAL, S3}
import pureconfig.generic.auto._

trait WithTestConfig {
  val alias = "public_database"

  private val sc: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/reference_kf.conf")
  implicit val conf: Configuration =
    sc
      .copy(datalake = sc.datalake.copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile, S3))))
}
