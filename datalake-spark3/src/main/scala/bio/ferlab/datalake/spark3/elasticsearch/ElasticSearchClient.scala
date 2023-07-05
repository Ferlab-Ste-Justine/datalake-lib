package bio.ferlab.datalake.spark3.elasticsearch

import bio.ferlab.datalake.spark3.utils.ResourceLoader.loadResource
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import sttp.client3.json4s._
import sttp.client3.logging.slf4j.Slf4jLoggingBackend
import sttp.client3.{SimpleHttpClient, UriContext, basicRequest}
import sttp.model.{MediaType, Uri}

class ElasticSearchClient(url: String, username: Option[String] = None, password: Option[String] = None) {

  private val indexUri: String => Uri = indexName => uri"$url/$indexName"
  private val templateUri: String => Uri = templateName => uri"$url/_index_template/$templateName"
  private val aliasesUri: Uri = uri"$url/_aliases"
  private val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)
  private val client = SimpleHttpClient().wrapBackend(Slf4jLoggingBackend(_))
  private val esUri: Uri = uri"$url"
  private implicit val serialization = org.json4s.jackson.Serialization
  private implicit val formats = org.json4s.DefaultFormats
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

    val request = basicRequest
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

    //    println(write(action))

    val request = basicRequest
      .post(aliasesUri)
      .contentType(MediaType.ApplicationJson)
      .body(action)

    client.send(request).body match {
      case Left(e) => throw new IllegalStateException(s"Server could not set alias to $alias, replied :$e")
      case _ => ()
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


}

