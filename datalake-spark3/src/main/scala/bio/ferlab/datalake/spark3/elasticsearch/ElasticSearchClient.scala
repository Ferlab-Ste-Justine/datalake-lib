package bio.ferlab.datalake.spark3.elasticsearch

import bio.ferlab.datalake.spark3.utils.ResourceLoader.loadResource
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import sttp.client3.json4s._
import sttp.client3.logging.slf4j.Slf4jLoggingBackend
import sttp.client3.{SimpleHttpClient, SttpClientException, UriContext, basicRequest}
import sttp.model.{MediaType, StatusCode, Uri}

class ElasticSearchClient(url: String, username: Option[String] = None, password: Option[String] = None) {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  private val indexUri: String => Uri = indexName => uri"$url/$indexName"
  private val templateUri: String => Uri = templateName => uri"$url/_index_template/$templateName"
  private val aliasesUri: Uri = uri"$url/_aliases"
  private val client = SimpleHttpClient().wrapBackend(Slf4jLoggingBackend(_))
  // for quiet none-blocking operations (no logging)
  private val quietClient = SimpleHttpClient()
  private val esUri: Uri = uri"$url"
  private implicit val serialization: Serialization.type = org.json4s.jackson.Serialization
  private implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
  private val esRequest =
    if (username.isDefined && password.isDefined)
      basicRequest.auth.basic(username.get, password.get)
    else
      basicRequest

  /**
   * Sends a GET on the url and verify the status code of the response is 200
   *
   * @return true if running
   *         false if not running or if status code not 200
   */
  def isRunning: Boolean = {
    client
      .send(esRequest.get(esUri))
      .isSuccess
  }

  /**
   * Check roles/http endpoint
   *
   * @return true if running
   *         false if not running or if status code not 200
   */
  def checkNodeRoles: Boolean = {
    client
      .send(esRequest.get(uri"$url/_nodes/http"))
      .isSuccess

  }

  /**
   * Set a template to ElasticSearch
   *
   * @param templatePath path of the template.json that is expected to be in the resource folder or spark
   * @throws IllegalStateException if the server could not set the template
   * @return the http response sent by ElasticSearch
   */
  def setTemplate(templatePath: String)(implicit spark: SparkSession): String = {
    val templateName = FilenameUtils.getBaseName(templatePath)

    // find template in resources first then with spark if failed
    val fileContent = loadResource(templatePath).getOrElse(spark.read.option("wholetext", "true").textFile(templatePath).collect().mkString)

    val request = esRequest
      .put(templateUri(templateName))
      .contentType(MediaType.ApplicationJson)
      .body(fileContent)

    val response = client.send(request)
    response.body match {
      case Left(e) => throw new IllegalStateException(s"Server could not set template and replied :${response.code + " : " + e}")
      case Right(r) => r
    }
  }

  /**
   * Set alias
   *
   * @param add    list of index to add to the alias
   * @param remove list of index to remove from the alias
   * @param alias  name of the alias to update
   * @throws IllegalStateException if the server could not set the alias
   */
  def setAlias(add: List[String], remove: List[String], alias: String): Unit = {

    val action = AliasActionsRequest(
      add.map(name => AddAction(Map("index" -> name, "alias" -> alias))) ++
        remove.map(name => RemoveAction(Map("index" -> name, "alias" -> alias)))
    )

    val request = esRequest
      .post(aliasesUri)
      .contentType(MediaType.ApplicationJson)
      .body(action)

    client.send(request).body match {
      case Left(e) => throw new IllegalStateException(s"Server could not set alias to $alias, replied :$e")
      case _ => ()
    }

  }

  /**
   * Get indices associates to a given alias
   *
   * @param aliasName name of the alias
   * @return a set of indices. Empty if alias does not exist.
   */
  def getAliasIndices(aliasName: String): Set[String] = {

    val aliasUrl = uri"$url/_alias/$aliasName"
    val request = esRequest
      .get(aliasUrl)
      .response(asJson[Map[String, Any]])
    val response = client.send(request)
    if (response.code == StatusCode.NotFound) {
      Set.empty
    } else {
      response.body match {
        case Left(e) => throw new IllegalStateException(s"Server could not get alias $aliasName, replied :$e")
        case Right(r) => r.keys.toSet
      }
    }


  }

  /**
   * Delete an index
   *
   * @throws IllegalStateException if the server could not delete the index
   * @param indexName name of the index to delete
   */
  def deleteIndex(indexName: String): Unit = {
    val request = esRequest
      .delete(indexUri(indexName).addParam("ignore_unavailable", "true"))
    client.send(request).body match {
      case Left(e) => throw new IllegalStateException(s"Server could not delete index and replied :$e")
      case _ => ()
    }
  }

  /**
   * Create an empty index. If an index template matches, the index will inherit its settings and mappings.
   * Idempotent: if the index already exists, this is a no-op (resource_already_exists_exception is treated as success).
   *
   * @param indexName name of the index to create
   * @throws IllegalStateException if the server could not create the index for any other reason
   */
  def createIndex(indexName: String): Unit = {
    val request = esRequest
      .put(indexUri(indexName))
      .contentType(MediaType.ApplicationJson)
      .body("{}")
    client.send(request).body.left.foreach {
      case e if e.contains("resource_already_exists_exception") => ()
      case e => throw new IllegalStateException(s"Server could not create index $indexName, replied: $e")
    }
  }

  /**
   * Get the current number_of_replicas setting for an index.
   *
   * @param indexName name of the index
   * @return Some(replica count as string) if the setting could be read, None otherwise.
   *         Callers should not assume a default — if None, the replica count is unknown
   *         and no restore attempt should be made.
   */
  def getNumberOfReplicas(indexName: String): Option[String] = {
    // Response with flat_settings=true: {"index_name": {"settings": {"index.number_of_replicas": "1"}}}
    val request = esRequest
      .get(uri"$url/$indexName/_settings/index.number_of_replicas".addParam("flat_settings", "true"))
      .response(asJson[Map[String, IndexSettingsBlock]])
    try {
      client.send(request).body.toOption
        .flatMap(_.get(indexName)) // select by key — defensive against wildcards/multi-match
        .flatMap(_.settings.get("index.number_of_replicas"))
    } catch {
      // Honor the None contract on connection/timeout failures so callers don't crash.
      case _: SttpClientException => None
    }
  }

  /**
   * Update settings on an existing index.
   *
   * @param indexName name of the index
   * @param settings  JSON string of settings to apply, e.g. {"index": {"number_of_replicas": "0"}}
   * @throws IllegalStateException if the server could not update settings
   */
  def setIndexSettings(indexName: String, settings: String): Unit = {
    val request = esRequest
      .put(uri"$url/$indexName/_settings")
      .contentType(MediaType.ApplicationJson)
      .body(settings)
    client.send(request).body.left.foreach(e =>
      throw new IllegalStateException(s"Server could not update settings for index $indexName, replied: $e"))
  }

  /**
   * Refresh an index, making all recent writes searchable.
   *
   * @param indexName name of the index to refresh
   * @throws IllegalStateException if the server could not refresh the index
   */
  def refreshIndex(indexName: String): Unit = {
    val request = esRequest.post(uri"$url/$indexName/_refresh")
    client.send(request).body.left.foreach(e =>
      throw new IllegalStateException(s"Server could not refresh index $indexName, replied: $e"))
  }

  /**
   * Trigger a force merge on an index. Intended for read-only indexes after bulk indexation —
   * it reduces segment count, reclaims disk from tombstones, and significantly improves query latency.
   *
   * ES's `_forcemerge` endpoint is synchronous at the HTTP level and can take 10+ minutes on
   * large indexes, which causes ingress/proxy timeouts. However, ES does NOT abort the merge
   * when the HTTP client disconnects — the operation continues on the server regardless.
   *
   * This method uses a short read timeout so the HTTP call returns quickly, and swallows any
   * timeout/error exception. The merge continues in the background on ES. Callers can inspect
   * progress via `GET /_tasks?actions=*forcemerge*&detailed=true`.
   *
   * This method is best-effort: failures are logged as warnings and never throw. Force merge
   * is a pure optimization — a failure should not break the indexation pipeline.
   *
   * @param indexName       name of the index to force merge
   * @param maxNumSegments  target segment count per shard (default: 1, ideal for read-only indexes)
   * @param readTimeout     HTTP read timeout — short by design, since we don't want to wait (default: 10 seconds)
   */
  def forceMergeIndex(indexName: String,
                      maxNumSegments: Int = 1,
                      readTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(10, "seconds")): Unit = {
    val request = esRequest
      .post(uri"$url/$indexName/_forcemerge".addParam("max_num_segments", maxNumSegments.toString))
      .readTimeout(readTimeout)
    try {
      quietClient.send(request).body.left.foreach(e =>
        log.warn(s"Force merge for index $indexName returned an error (merge may still be running on ES): $e"))
    } catch {
      // Read timeout is the expected path for large indexes — ES continues the merge in background
      // when the client disconnects. Other sttp failures (connect refused, DNS, etc.) are also swallowed:
      // force merge is best-effort optimization, never break the pipeline.
      case e: SttpClientException =>
        log.warn(s"Force merge for index $indexName failed (${e.getClass.getSimpleName}: ${e.getMessage}) — best-effort, ignoring")
    }
  }

}

