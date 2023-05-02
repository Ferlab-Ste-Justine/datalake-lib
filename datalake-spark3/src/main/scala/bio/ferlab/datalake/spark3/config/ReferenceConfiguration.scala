package bio.ferlab.datalake.spark3.config

import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import bio.ferlab.datalake.spark3.genomics.GenomicDatasets
import bio.ferlab.datalake.spark3.publictables.PublicDatasets

import pureconfig.generic.auto._
object ReferenceConfiguration extends App {

  val alias = "public_database"

  val prod_storage = List(
    StorageConf(alias, "s3a://kf-strides-variant-parquet-prd", S3)
  )

  val kf_conf =
    SimpleConfiguration(
      DatalakeConf(
        storages = prod_storage,
        sources = PublicDatasets(alias, Some("variant"), Some("variant_live")).sources ++ GenomicDatasets(alias, Some("variant"), Some("variant_live")).sources,
        sparkconf = Map("hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
      )
    )


  ConfigurationWriter.writeTo("datalake-spark3/src/main/resources/reference_kf.conf", kf_conf)
  ConfigurationWriter.writeTo("datalake-spark3/src/test/resources/config/reference_kf.conf", kf_conf)

}
