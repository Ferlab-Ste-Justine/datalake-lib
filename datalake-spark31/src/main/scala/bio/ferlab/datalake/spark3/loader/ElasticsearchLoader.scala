package bio.ferlab.datalake.spark3.loader
import bio.ferlab.datalake.spark3.elasticsearch.{ElasticSearchClient, EsWriteOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

import java.time.LocalDate
import scala.util.Try

object ElasticsearchLoader extends Loader {

  /**
   * Default read logic for a loader
   *
   * @param location     absolute path of where the data is
   * @param format       string representing the format
   * @param readOptions  read options
   * @param databaseName Optional database name
   * @param tableName    Optional table name
   * @param spark        spark session
   * @return the data as a dataframe
   */
  override def read(location: String,
                    format: String,
                    readOptions: Map[String, String],
                    databaseName: Option[String],
                    tableName: Option[String])(implicit spark: SparkSession): DataFrame = {
    spark.sqlContext.read.format("es").load(location)
  }

  /**
   * Keeps old partition and overwrite new partitions.
   *
   * @param location     where to write the data
   * @param databaseName database name
   * @param tableName    table name
   * @param df           new data to write into the table
   * @param partitioning how the data is partitionned
   * @param format       format
   * @param options      write options
   * @param spark        a spark session
   * @return updated data
   */
  override def overwritePartition(location: String,
                                  databaseName: String,
                                  tableName: String,
                                  df: DataFrame,
                                  partitioning: List[String],
                                  format: String,
                                  options: Map[String, String])(implicit spark: SparkSession): DataFrame = {

    val es_url: String = options(EsWriteOptions.ES_URL)
    val es_username: Option[String] = options.get(EsWriteOptions.ES_USERNAME)
    val es_password: Option[String] = options.get(EsWriteOptions.ES_PASSWORD)
    implicit val esClient: ElasticSearchClient = new ElasticSearchClient(es_url, es_username, es_password)
    val ES_config = Map("es.write.operation"-> "index")

    options.get(EsWriteOptions.ES_INDEX_TEMPLATE_PATH).foreach(path => setupIndex(tableName, path))

    df.saveToEs(s"$location/_doc", ES_config)
    publish(tableName, location)
    df
  }

  /**
   * Overwrites the data located in output/tableName
   * usually used for small/test tables.
   *
   * @param location     where to write the data
   * @param databaseName database name
   * @param tableName    table name
   * @param df           new data to write into the table
   * @param partitioning how the data is partitionned
   * @param format       format
   * @param options      write options
   * @param spark        a spark session
   * @return updated data
   */
  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: List[String],
                         format: String,
                         options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

  /**
   * Insert or append data into a table
   * Does not resolve duplicates
   *
   * @param location     full path of where the data will be located
   * @param databaseName database name
   * @param tableName    the name of the updated/created table
   * @param updates      new data to be merged with existing data
   * @param partitioning how the data should be partitioned
   * @param format       spark form
   * @param options      write options
   * @param spark        a valid spark session
   * @return the data as a dataframe
   */
  override def insert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

  /**
   * Update or insert data into a table
   * Resolves duplicates by using the list of primary key passed as argument
   *
   * @param location    full path of where the data will be located
   * @param tableName   the name of the updated/created table
   * @param updates     new data to be merged with existing data
   * @param primaryKeys name of the columns holding the unique id
   * @param spark       a valid spark session
   * @return the data as a dataframe
   */
override def upsert(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    partitioning: List[String],
                    format: String,
                    options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table for which keeping the full historic is not required.
   *
   * @param location      full path of where the data will be located
   * @param tableName     the name of the updated/created table
   * @param updates       new data to be merged with existing data
   * @param primaryKeys   name of the columns holding the unique id
   * @param oidName       name of the column holding the hash of the column that can change over time (or version number)
   * @param createdOnName name of the column holding the creation timestamp
   * @param updatedOnName name of the column holding the last update timestamp
   * @param spark         a valid spark session
   * @return the data as a dataframe
   */
override def scd1(location: String,
                  databaseName: String,
                  tableName: String,
                  updates: DataFrame,
                  primaryKeys: Seq[String],
                  oidName: String,
                  createdOnName: String,
                  updatedOnName: String,
                  partitioning: List[String],
                  format: String)(implicit spark: SparkSession): DataFrame = ???

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table for which keeping the full historic is required.
   *
   * @param location      full path of where the data will be located
   * @param tableName     the name of the updated/created table
   * @param updates       new data to be merged with existing data
   * @param primaryKeys   name of the columns holding the unique id
   * @param oidName       name of the column holding the hash of the column that can change over time (or version number)
   * @param createdOnName name of the column holding the creation timestamp
   * @param updatedOnName name of the column holding the last update timestamp
   * @param spark         a valid spark session
   * @return the data as a dataframe
   */
override def scd2(location: String,
                  databaseName: String,
                  tableName: String,
                  updates: DataFrame,
                  primaryKeys: Seq[String],
                  buidName: String,
                  oidName: String,
                  isCurrentName: String,
                  partitioning: List[String],
                  format: String,
                  validFromName: String,
                  validToName: String,
                  minValidFromDate: LocalDate,
                  maxValidToDate: LocalDate)(implicit spark: SparkSession): DataFrame = ???

  def publish(alias: String,
              currentIndex: String,
              previousIndex: Option[String] = None)(implicit esClient: ElasticSearchClient): Unit = {
    Try(esClient.setAlias(add = List(currentIndex), remove = List(), alias))
      .foreach(_ => log.info(s"${currentIndex} added to $alias"))
    Try(esClient.setAlias(add = List(), remove = previousIndex.toList, alias))
      .foreach(_ => log.info(s"${previousIndex.toList.mkString} removed from $alias"))
  }


  /**
   * Setup an index by checking that ES nodes are up, removing the old index and setting the template for this index.
   *
   * @param indexName full index name
   * @param templateFilePath absolute path of the template file, it will be read as a whole file by Spark.
   * @param esClient an instance of [[ElasticSearchClient]]
   */
  def setupIndex(indexName: String, templateFilePath: String)
                (implicit spark: SparkSession, esClient: ElasticSearchClient): Unit = {
    Try {
      log.info(s"ElasticSearch 'isRunning' status: [${esClient.isRunning}]")
      log.info(s"ElasticSearch 'checkNodes' status: [${esClient.checkNodeRoles}]")

      val respDelete = esClient.deleteIndex(indexName)
      log.info(s"DELETE INDEX[$indexName] : " + respDelete.getStatusLine.getStatusCode + " : " + respDelete.getStatusLine.getReasonPhrase)
    }
    val response = esClient.setTemplate(templateFilePath)
    log.info(s"SET TEMPLATE[${templateFilePath}] : " + response.getStatusLine.getStatusCode + " : " + response.getStatusLine.getReasonPhrase)
  }
}
