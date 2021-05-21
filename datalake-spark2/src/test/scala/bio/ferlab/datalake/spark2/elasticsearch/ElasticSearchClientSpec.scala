package bio.ferlab.datalake.spark2.elasticsearch

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.util.Try

class ElasticSearchClientSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  val indexName: String = "test"
  val alias: String = "alias"
  val templateFileName: String = getClass.getClassLoader.getResource("template_example.json").getFile
  val esUrl = "http://localhost:9200"
  val esClient = new ElasticSearchClient(esUrl)

  Try(esClient.deleteIndex(indexName))

  "ES instance" should s"create/delete index and template and set aliases" in {
    esClient.isRunning shouldBe true
    esClient.createIndex(indexName).getStatusLine.getStatusCode shouldBe 200
    esClient.setAlias(List(indexName), List(), alias).getStatusLine.getStatusCode shouldBe 200
    esClient.getIndex(alias).getStatusLine.getStatusCode shouldBe 200
    esClient.deleteIndex(indexName).getStatusLine.getStatusCode shouldBe 200
    esClient.setTemplate(templateFileName).getStatusLine.getStatusCode shouldBe 200
    esClient.deleteTemplate("template_example").getStatusLine.getStatusCode shouldBe 200
  }

}

