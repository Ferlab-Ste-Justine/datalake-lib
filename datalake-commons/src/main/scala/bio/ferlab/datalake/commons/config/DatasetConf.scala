package bio.ferlab.datalake.commons.config

/**
 * Abstraction on a dataset configuration
 *
 * @param storageid         an alias designating where the data is sitting.
 *                          this can point to an object store url in the configuration like s3://my-bucket/
 * @param path              the relative path from the root of the storage to the dataset. ie, /raw/my-system/my-source
 * @param table             OPTIONAL - configuration of a table associated to the dataset
 * @param format            data format
 * @param loadtype          how the data is written
 * @param readoptions       OPTIONAL - read options to pass to spark in order to read the data into a DataFrame
 * @param writeoptions      OPTIONAL - write options to pass to spark in order to write the data into files
 * @param documentationpath OPTIONAL - where the documentation is located.
 * @param view              OPTIONAL - schema of the view pointing to the concrete table
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
                       writeoptions: Map[String, String] = WriteOptions.DEFAULT_OPTIONS,
                       documentationpath: Option[String] = None,
                       view: Option[TableConf] = None,
                       repartition: Option[Repartition] = None) {
  self =>


  /**
   * A dataset UID is the column representing the unique identifier.
   * If the dataset defines a table the uid column name is the table name followed by '_uid'.
   * In most cases the uid is the hash of the [[keys]].
   *
   * @return
   */
  def uid: String = table.map(t => s"${t.name}_uid").getOrElse("uid")

  /**
   * A dataset BUID is the column representing the business identifier.
   * If the dataset defines a table the buid column name is the table name followed by '_buid'.
   *
   * @return
   */
  def buid: String = table.map(t => s"${t.name}_buid").getOrElse("buid")

  /**
   * A dataset OID is the column representing the object identifier, in other words: it is a hash representing all the column expect the BUID.
   * If the dataset defines a table the oid column name is the table name followed by '_oid'.
   *
   * @return
   */
  def oid: String = table.map(t => s"${t.name}_oid").getOrElse("oid")

  /**
   * The absolute path of the root of the storage where the dataset is stored.
   *
   * @param config configuration currently loaded
   * @return
   */
  def rootPath(implicit config: Configuration): String = {
    config.getStorage(storageid).path
  }

  /**
   * the absolute path where the dataset is stored
   *
   * @param config configuration currently loaded
   * @return
   */
  def location(implicit config: Configuration): String = {
    s"$rootPath$path"
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
            keys: List[String],
            table: TableConf,
            view: TableConf): DatasetConf = {
    new DatasetConf(
      id,
      storageid,
      path,
      format,
      loadtype,
      keys = keys,
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

  def apply(id: String,
            storageid: String,
            path: String,
            format: Format,
            loadtype: LoadType,
            keys: List[String],
            table: TableConf,
            view: TableConf,
            repartition: Repartition): DatasetConf = {
    new DatasetConf(
      id,
      storageid,
      path,
      format,
      loadtype,
      keys = keys,
      table = Some(table),
      view = Some(view),
      repartition = Some(repartition)
    )
  }
}
