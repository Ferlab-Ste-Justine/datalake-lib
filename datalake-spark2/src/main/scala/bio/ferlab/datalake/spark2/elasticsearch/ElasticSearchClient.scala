package bio.ferlab.datalake.spark2.elasticsearch

import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpResponse}
import org.apache.spark.sql.SparkSession

class ElasticSearchClient(url: String) {

  /**
   * Sends a GET on the url and verify the status code of the response is 200
   * @return true if running
   *         false if not running or if status code not 200
   */
  def isRunning: Boolean = {
    val response = new DefaultHttpClient().execute(new HttpGet(url))

    println(s"""
               |GET $url
               |${response.toString}
               |${EntityUtils.toString(response.getEntity)}
               |""".stripMargin)
    response.getStatusLine.getStatusCode == 200
  }

  /**
   * Check roles/http endpoint
   * @return true if running
   *         false if not running or if status code not 200
   */
  def checkNodeRoles: Boolean = {
    val response = new DefaultHttpClient().execute(new HttpGet(url + "/_nodes/http"))

    println(s"""
               |GET $url/_nodes/http
               |${response.toString}
               |${EntityUtils.toString(response.getEntity)}
               |""".stripMargin)
    response.getStatusLine.getStatusCode == 200
  }

  /**
   * Set a template to ElasticSearch
   * @param templatePath path of the template.json that is expected to be in the resource folder
   * @param templateName name for the template
   * @return the http response sent by ElasticSearch
   */
  def setTemplate(templatePath: String)(implicit spark: SparkSession): HttpResponse = {

    val templateName = templatePath.split('.').dropRight(1).last.split('/').last
    val requestUrl = s"$url/_template/$templateName"

    val fileContent = spark.read.option("wholetext", "true").textFile(templatePath).collect().mkString

    println(s"SENDING: PUT $requestUrl with content: $fileContent")

    val request = new HttpPut(requestUrl)
    request.addHeader(HttpHeaders.CONTENT_TYPE,"application/json")
    request.setEntity(new StringEntity(fileContent))
    val response = new DefaultHttpClient().execute(request)
    val status = response.getStatusLine
    if (!status.getStatusCode.equals(200))
      throw new Exception(s"Server could not set template [$templatePath] and replied :${status.getStatusCode + " : " + status.getReasonPhrase}")
    response
  }

  /**
   * Set alias
   * @param indexName name of the name to add to the alias
   * @param aliasName name of the alias to update
   * @return the http response sent by ElasticSearch
   */
  def setAlias(indexName: String, aliasName: String): HttpResponse = {

    val requestUrl = s"$url/_aliases"

    println(s"UPDATING ALIAS: ADD ${indexName} to $aliasName")

    val body =
      s"""
         |{
         |	"actions": [{
         |		"add": {
         |			"index": "$indexName",
         |			"alias": "$aliasName"
         |		}
         |	}]
         |}
         |""".stripMargin

    val request = new HttpPost(requestUrl)
    request.addHeader(HttpHeaders.CONTENT_TYPE,"application/json")
    request.setEntity(new StringEntity(body))
    val response = new DefaultHttpClient().execute(request)
    val status = response.getStatusLine
    if (!status.getStatusCode.equals(200))
      throw new Exception(s"Server could not add [$indexName] to $aliasName and replied :${status.getStatusCode + " : " + status.getReasonPhrase}")
    response
  }

  /**
   * Delete a template
   * @param templateName name of the template to delete
   * @return the http response sent by ElasticSearch
   */
  def deleteTemplate(templateName: String): HttpResponse = {
    val requestUrl = s"$url/_template/$templateName"
    val request = new HttpDelete(requestUrl)
    val response = new DefaultHttpClient().execute(request)
    response
  }

  /**
   * Create an index
   * @param indexName name of the index to create
   * @return the http response sent by ElasticSearch
   */
  def createIndex(indexName: String): HttpResponse = {
    val requestUrl = s"$url/$indexName"
    val request = new HttpPut(requestUrl)
    val response = new DefaultHttpClient().execute(request)
    response
  }

  /**
   * Delete an index
   * @param indexName name of the index to delete
   * @return the http response sent by ElasticSearch
   */
  def deleteIndex(indexName: String): HttpResponse = {
    val requestUrl = s"$url/$indexName"
    val request = new HttpDelete(requestUrl)
    val response = new DefaultHttpClient().execute(request)
    response
  }

}

