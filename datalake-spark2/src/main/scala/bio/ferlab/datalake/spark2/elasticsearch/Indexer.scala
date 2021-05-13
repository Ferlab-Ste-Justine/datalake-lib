package bio.ferlab.datalake.spark2.elasticsearch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.elasticsearch.spark.sql._

import scala.util.Try

object Indexer extends App {

  implicit val spark: SparkSession = SparkSession.builder
    .config("es.index.auto.create", "true")
    .config("es.nodes", args(1))
    .config("es.nodes.client.only", "false")
    .config("es.nodes.discovery", "false")
    .config("es.nodes.wan.only", "true")
    .config("es.read.ignore_exception",  "true")
    .config("es.port", "443")
    .config("es.wan.only", "true")
    .config("es.write.ignore_exception", "true")

    .config("spark.es.nodes.client.only", "false")
    .config("spark.es.nodes.wan.only", "true")
    .appName(s"Indexer").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(input, esNodes, indexName, release, templateFileName, jobType, columnId, chromosome, format) = args

  val ES_config =
    Map(
      //"es.mapping.id" -> columnId,
      "es.write.operation"-> jobType)

  val esClient = new ElasticSearchClient(esNodes.split(',').head)

  chromosome match {
    case "all" =>
      val index = s"${indexName}_$release".toLowerCase
      if (jobType == "index") setupIndex(index)
      spark.read
        .format(format)
        .load(input)
        .repartition(200)
        .saveToEs(s"$index/_doc", ES_config)

      Try(esClient.setAlias(index, indexName))

    case s =>
      val index = s"${indexName}_${release}_${s}".toLowerCase
      if (jobType == "index") setupIndex(index)
      spark.read
        .format(format)
        .load(input)
        .where(col("chromosome") === s)
        .repartition(200)
        .saveToEs(s"$index/_doc", ES_config)

      Try(esClient.setAlias(index, indexName))
  }



  def setupIndex(indexName: String): Unit = {
    Try {
      println(s"ElasticSearch 'isRunning' status: [${esClient.isRunning}]")
      println(s"ElasticSearch 'checkNodes' status: [${esClient.checkNodeRoles}]")

      val respDelete = esClient.deleteIndex(indexName)
      println(s"DELETE INDEX[$indexName] : " + respDelete.getStatusLine.getStatusCode + " : " + respDelete.getStatusLine.getReasonPhrase)
    }
    val response = esClient.setTemplate(s"s3://kf-strides-variant-parquet-prd/jobs/templates/$templateFileName")
    println(s"SET TEMPLATE[${templateFileName}] : " + response.getStatusLine.getStatusCode + " : " + response.getStatusLine.getReasonPhrase)
  }
}

