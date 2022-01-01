package bio.ferlab.datalake.spark3.elasticsearch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
import org.slf4j

import scala.util.Try

/**
 *
 * @param spark instantiated spark session
 * {{{
 * val spark: SparkSession = SparkSession.builder
    .config("es.index.auto.create", "true")
    .config("es.nodes", "http://es_nodes_url")
    .config("es.nodes.client.only", "false")
    .config("es.nodes.discovery", "false")
    .config("es.nodes.wan.only", "true")
    .config("es.read.ignore_exception",  "true")
    .config("es.port", "443")
    .config("es.wan.only", "true")
    .config("es.write.ignore_exception", "true")

    .config("spark.es.nodes.client.only", "false")
    .config("spark.es.nodes.wan.only", "true")
    .appName(s"Indexer")
    .getOrCreate()
 * }}}
 *
 */
class Indexer(jobType: String,
              templateFilePath: String,
              currentIndex: String)
             (implicit val spark: SparkSession) {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def run(df: DataFrame)(implicit esClient: ElasticSearchClient): Unit = {
    val ES_config = Map("es.write.operation"-> jobType)

    if (jobType == "index") setupIndex(currentIndex, templateFilePath)

    df.saveToEs(s"$currentIndex/_doc", ES_config)
  }

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
  def setupIndex(indexName: String, templateFilePath: String)(implicit esClient: ElasticSearchClient): Unit = {
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

