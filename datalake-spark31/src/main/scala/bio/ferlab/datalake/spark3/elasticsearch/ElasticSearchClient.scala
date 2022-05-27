package bio.ferlab.datalake.spark3.elasticsearch

import bio.ferlab.datalake.spark3.utils.ResourceLoader.loadResource
import org.apache.commons.io.FilenameUtils
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.protocol.HttpContext
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpRequest, HttpRequestInterceptor, HttpResponse}
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.slf4j.{Logger, LoggerFactory}
import org.sparkproject.guava.io.BaseEncoding

import java.nio.charset.StandardCharsets

class ElasticSearchClient(url: String, username: Option[String] = None, password: Option[String] = None) {

  private val indexUrl: String => String = indexName => s"$url/$indexName"
  private val templateUrl: String => String = templateName => s"$url/_index_template/$templateName"
  private val aliasesUrl: String = s"$url/_aliases"
  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  def http: CloseableHttpClient = {
    val client = HttpClientBuilder.create()

    if(username.isDefined && password.isDefined) {
      client.addInterceptorFirst(new HttpRequestInterceptor {
        override def process(request: HttpRequest, context: HttpContext): Unit = {
          val auth = s"${username.get}:${password.get}"
          request.addHeader(
            "Authorization",
            s"Basic ${BaseEncoding.base64().encode(auth.getBytes(StandardCharsets.UTF_8))}"
          )
        }
      })
    }
    client.build()
  }

  /**
   * Sends a GET on the url and verify the status code of the response is 200
   * @return true if running
   *         false if not running or if status code not 200
   */
  def isRunning: Boolean = {
    val response = http.execute(new HttpGet(url))

    log.info(s"""
               |GET $url
               |${response.toString}
               |${EntityUtils.toString(response.getEntity)}
               |""".stripMargin)
    http.close()
    response.getStatusLine.getStatusCode == 200
  }

  /**
   * Check roles/http endpoint
   * @return true if running
   *         false if not running or if status code not 200
   */
  def checkNodeRoles: Boolean = {
    val response = http.execute(new HttpGet(url + "/_nodes/http"))

    log.info(s"""
               |GET $url/_nodes/http
               |${response.toString}
               |${EntityUtils.toString(response.getEntity)}
               |""".stripMargin)
    http.close()
    response.getStatusLine.getStatusCode == 200
  }

  /**
   * Set a template to ElasticSearch
   * @param templateResourcePath resource path of the template
   * @return the http response sent by ElasticSearch
   */
  def setTemplate(templateResourcePath: String)(implicit spark: SparkSession): HttpResponse = {
    val templateName = FilenameUtils.getBaseName(templateResourcePath)

    val fileContent = loadResource(templateResourcePath)

    log.info(s"SENDING: PUT ${templateUrl(templateName)} with content: $fileContent")

    val request = new HttpPut(templateUrl(templateName))
    request.addHeader(HttpHeaders.CONTENT_TYPE,"application/json")
    request.setEntity(new StringEntity(fileContent))
    val response = http.execute(request)
    val status = response.getStatusLine
    if (!status.getStatusCode.equals(200))
      throw new Exception(s"Server could not set template and replied :${status.getStatusCode + " : " + status.getReasonPhrase}")
    http.close()
    response
  }

  /**
   * Set alias
   * @param indexName name of the name to add to the alias
   * @param aliasName name of the alias to update
   * @return the http response sent by ElasticSearch
   */
  def setAlias(add: List[String], remove: List[String], alias: String): HttpResponse = {

    val action = ActionsRequest(
      add.map(name => AddAction(Map("index" -> name, "alias" -> alias))) ++
        remove.map(name => RemoveAction(Map("index" -> name, "alias" -> alias)))
    )

    implicit val formats = DefaultFormats

    val requestBody = write(action)

    log.info(requestBody)

    val request = new HttpPost(aliasesUrl)
    request.addHeader(HttpHeaders.CONTENT_TYPE,"application/json")
    request.setEntity(new StringEntity(requestBody))

    val response = http.execute(request)
    val status = response.getStatusLine
    if (!status.getStatusCode.equals(200))
      throw new Exception(s"Server could not set alias to $alias and replied :${status.getStatusCode + " : " + status.getReasonPhrase}")
    http.close()
    response
  }

  /**
   * Delete a template
   * @param templateName name of the template to delete
   * @return the http response sent by ElasticSearch
   */
  def deleteTemplate(templateName: String): HttpResponse = {
    val response = http.execute(new HttpDelete(templateUrl(templateName)))
    http.close()
    response
  }

  /**
   * Get an index
   * @param indexName name of the index to fetch
   * @return the http response sent by ElasticSearch
   */
  def getIndex(indexName: String): HttpResponse = {
    val response = http.execute(new HttpGet(indexUrl(indexName)))
    http.close()
    response
  }

  /**
   * Create an index
   * @param indexName name of the index to create
   * @return the http response sent by ElasticSearch
   */
  def createIndex(indexName: String): HttpResponse = {
    val response = http.execute(new HttpPut(indexUrl(indexName)))
    http.close()
    response
  }

  /**
   * Delete an index
   * @param indexName name of the index to delete
   * @return the http response sent by ElasticSearch
   */
  def deleteIndex(indexName: String): HttpResponse = {
    val response = http.execute(new HttpDelete(indexUrl(indexName)))
    http.close()
    response
  }

  sealed trait Action
  case class AddAction(add: Map[String, String]) extends Action
  case class RemoveAction(remove: Map[String, String]) extends Action
  case class ActionsRequest(actions: Seq[Action])

}

