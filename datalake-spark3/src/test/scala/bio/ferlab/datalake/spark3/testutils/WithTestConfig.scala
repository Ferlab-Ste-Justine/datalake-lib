package bio.ferlab.datalake.spark3.testutils

import bio.ferlab.datalake.commons.config.{ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3

trait WithTestConfig {
  val alias = "public_database"

  private val sc: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/reference_kf.conf")
  implicit val conf: SimpleConfiguration =
    sc
      .copy(datalake = sc.datalake.copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile, S3))))
}
