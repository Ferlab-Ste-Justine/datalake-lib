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
              disableReplicas: Boolean = false,
              forceMerge: Boolean = false)
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
    val esConfig = Map(
      "es.write.operation" -> jobType,
      "es.batch.write.refresh" -> "false"
    ) ++ esWriteConfig

    if (jobType == "index") setupIndex(currentIndex, templateFilePath)

    withReplicasDisabled(currentIndex, disableReplicas) {
      df.saveToEs(s"$currentIndex/_doc", esConfig)
      log.info(s"Refreshing index [$currentIndex]")
      esClient.refreshIndex(currentIndex)
    }

    // Force merge runs AFTER replicas are restored so it operates on every shard copy.
    // If write failed above, the exception propagates and we never reach this line — correct, no point merging a failed index.
    if (forceMerge) {
      log.info(s"Triggering async force merge for index [$currentIndex] (runs in background on ES)")
      esClient.forceMergeIndex(currentIndex, maxNumSegments = 1)
    }
  }

  /**
   * Runs `block` with the index's replicas temporarily set to 0 (when `enabled`),
   * then restores the original replica count — even if `block` throws.
   *
   * If the original replica count cannot be read or the disable call fails, the optimization
   * is skipped and `block` runs normally with no restore attempt (avoids leaving the index
   * at number_of_replicas=0).
   */
  private def withReplicasDisabled(indexName: String, enabled: Boolean)(block: => Unit)
                                  (implicit esClient: ElasticSearchClient): Unit = {
    val originalReplicas: Option[String] = if (enabled) disableReplicas(indexName) else None
    try block
    finally originalReplicas.foreach(restoreReplicas(indexName, _))
  }

  private def disableReplicas(indexName: String)(implicit esClient: ElasticSearchClient): Option[String] = {
    // Create the empty index first so we can modify its settings before writing data.
    // Without this, the index only exists after saveToEs writes the first document (via es.index.auto.create).
    esClient.createIndex(indexName)
    esClient.getNumberOfReplicas(indexName) match {
      case None =>
        log.warn(s"Could not read current replica count for index [$indexName], skipping replica optimization to avoid incorrect restore")
        None
      case Some(current) =>
        log.info(s"Disabling replicas for index [$indexName] during write (was: $current)")
        try {
          esClient.setIndexSettings(indexName, """{"index": {"number_of_replicas": "0"}}""")
          Some(current)
        } catch {
          case e: Throwable =>
            log.warn(s"Failed to disable replicas for index [$indexName], proceeding without replica optimization: ${e.getMessage}")
            None
        }
    }
  }

  private def restoreReplicas(indexName: String, replicas: String)(implicit esClient: ElasticSearchClient): Unit = {
    log.info(s"Restoring replicas for index [$indexName] to $replicas")
    try esClient.setIndexSettings(indexName, s"""{"index": {"number_of_replicas": "$replicas"}}""")
    catch {
      case e: Throwable =>
        log.error(s"Failed to restore replicas for index [$indexName] to $replicas — index left at number_of_replicas=0, manual intervention required", e)
        throw e
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

