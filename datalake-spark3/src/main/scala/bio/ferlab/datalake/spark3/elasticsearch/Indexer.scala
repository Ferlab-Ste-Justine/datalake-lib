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
              currentIndex: String,
              disableReplicas: Boolean = false)
             (implicit val spark: SparkSession) {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * @param df            DataFrame to index
   * @param esWriteConfig optional ES write settings to override defaults (e.g. es.batch.size.bytes, es.batch.size.entries).
   *                      The only default override applied by the library is es.batch.write.refresh=false (paired with
   *                      a single manual refresh after write). Callers should tune batch sizes for their ES cluster.
   */
  def run(df: DataFrame, esWriteConfig: Map[String, String] = Map.empty)(implicit esClient: ElasticSearchClient): Unit = {
    val defaultConfig = Map(
      "es.write.operation" -> jobType,
      "es.batch.write.refresh" -> "false"
    )
    val ES_config = defaultConfig ++ esWriteConfig

    if (jobType == "index") setupIndex(currentIndex, templateFilePath)

    // When disabling replicas, create the empty index first so we can modify its settings before writing data.
    // Without this, the index only exists after saveToEs writes the first document (via es.index.auto.create).
    val originalReplicas = if (disableReplicas) {
      esClient.createIndex(currentIndex)
      val current = esClient.getNumberOfReplicas(currentIndex)
      log.info(s"Disabling replicas for index [$currentIndex] during write (was: $current)")
      esClient.setIndexSettings(currentIndex, """{"index": {"number_of_replicas": "0"}}""")
      Some(current)
    } else None

    df.saveToEs(s"$currentIndex/_doc", ES_config)

    log.info(s"Refreshing index [$currentIndex]")
    esClient.refreshIndex(currentIndex)

    originalReplicas.foreach { replicas =>
      log.info(s"Restoring replicas for index [$currentIndex] to $replicas")
      esClient.setIndexSettings(currentIndex, s"""{"index": {"number_of_replicas": "$replicas"}}""")
    }
  }

  def publish(alias: String,
              currentIndex: String,
              previousIndex: Option[String] = None)(implicit esClient: ElasticSearchClient): Unit = {
    esClient.setAlias(add = List(currentIndex), remove = previousIndex.toList, alias)
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

