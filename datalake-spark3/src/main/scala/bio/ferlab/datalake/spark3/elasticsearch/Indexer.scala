package bio.ferlab.datalake.spark3.elasticsearch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
import org.slf4j

/**
 *
 * @param spark instantiated spark session
 * {{{
 * val spark: SparkSession = SparkSession.builder
 *.config("es.index.auto.create", "true")
 *.config("es.nodes", "http://es_nodes_url")
 *.config("es.nodes.client.only", "false")
 *.config("es.nodes.discovery", "false")
 *.config("es.nodes.wan.only", "true")
 *.config("es.read.ignore_exception",  "true")
 *.config("es.port", "443")
 *.config("es.wan.only", "true")
 *.config("es.write.ignore_exception", "true")
 *
 *.config("spark.es.nodes.client.only", "false")
 *.config("spark.es.nodes.wan.only", "true")
 *.appName(s"Indexer")
 *.getOrCreate()
 * }}}
 *
 */
@deprecated
class Indexer(jobType: String,
              templateFilePath: String,
              currentIndex: String)
             (implicit val spark: SparkSession) {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def run(df: DataFrame)(implicit esClient: ElasticSearchClient): Unit = {
    val ES_config = Map("es.write.operation" -> jobType)

    if (jobType == "index") setupIndex(currentIndex, templateFilePath)

    df.saveToEs(s"$currentIndex/_doc", ES_config)
  }

  def publish(alias: String,
              currentIndex: String,
              previousIndex: Option[String] = None)(implicit esClient: ElasticSearchClient): Unit = {
    esClient.setAlias(add = List(currentIndex), remove = List(), alias)
    esClient.setAlias(add = List(), remove = previousIndex.toList, alias)
  }


  /**
   * Setup an index by checking that ES nodes are up, removing the old index and setting the template for this index.
   *
   * @param indexName    full index name
   * @param templatePath path of the template.json that is expected to be in the resource folder or spark
   * @param esClient     an instance of [[ElasticSearchClient]]
   */
  def setupIndex(indexName: String, templatePath: String)(implicit esClient: ElasticSearchClient): Unit = {
    log.info(s"ElasticSearch 'isRunning' status: [${esClient.isRunning}]")
    log.info(s"ElasticSearch 'checkNodes' status: [${esClient.checkNodeRoles}]")

    esClient.deleteIndex(indexName)
    esClient.setTemplate(templatePath)

  }
}

