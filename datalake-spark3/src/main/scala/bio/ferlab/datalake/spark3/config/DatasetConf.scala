package bio.ferlab.datalake.spark3.config

import bio.ferlab.datalake.spark3.loader.{Format, LoadType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Abstraction on a dataset configuration
 * @param storageid an alias designating where the data is sitting.
 *                     this can point to an object store url in the configuration like s3://my-bucket/
 * @param path the relative path from the root of the storage to the dataset. ie, /raw/my-system/my-source
 * @param table OPTIONAL - configuration of a table associated to the dataset
 * @param format data format
 * @param loadtype how the data is written
 * @param readoptions OPTIONAL - read options to pass to spark in order to read the data into a DataFrame
 * @param writeoptions OPTIONAL - write options to pass to spark in order to write the data into files
 * @param documentationpath OPTIONAL - path to the documentation file
 * @param view OPTIONAL - schema of the view pointing to the concrete table
 */
case class DatasetConf(id: String,
                       storageid: String,
                       path: String,
                       format: Format,
                       loadtype: LoadType,
                       table: Option[TableConf] = None,
                       keys: List[String] = List(),
                       partitionby: List[String] = List(),
                       readoptions: Map[String, String] = Map(),
                       writeoptions: Map[String, String] = Map(),
                       documentationpath: String = "",
                       view: Option[TableConf] = None) {

  def rootPath(implicit config: Configuration): String = {
    config.getStorage(storageid)
  }

  def location(implicit config: Configuration): String = {
    s"$rootPath$path"
  }

  def read(implicit config: Configuration, spark: SparkSession): DataFrame = {
    table.fold {
      spark.read.format(format.sparkFormat).options(readoptions).load(location)
    }{t =>
      spark.table(t.fullName)
    }
  }
}

object DatasetConf {
  def apply(id: String,
            storageid: String,
            path: String,
            format: Format,
            loadtype: LoadType,
            table: TableConf,
            view: TableConf): DatasetConf = {
    new DatasetConf(
      id,
      storageid,
      path,
      format,
      loadtype,
      table = Some(table),
      view = Some(view)
    )
  }

  def apply(id: String,
            storageid: String,
            path: String,
            format: Format,
            loadtype: LoadType,
            table: TableConf): DatasetConf = {
    new DatasetConf(
      id,
      storageid,
      path,
      format,
      loadtype,
      table = Some(table)
    )
  }
}
