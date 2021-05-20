package bio.ferlab.datalake.spark2.elasticsearch

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.util.Random

class ElasticSearchClientSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  val indexName: String = Random.nextString(20)
  val templateFileName: String = getClass.getClassLoader.getResource("template_example.json").getFile
  val esUrl = "http://localhost:9200"
  val esClient = new ElasticSearchClient(esUrl)

  "ES instance" should s"be up on $esUrl" in {
    esClient.isRunning shouldBe true
  }

  "ES client" should "create then delete index" in {
    esClient.createIndex(indexName).getStatusLine.getStatusCode shouldBe 200
    esClient.deleteIndex(indexName).getStatusLine.getStatusCode shouldBe 200
  }

  "ES client" should "create then delete template" in {
    esClient.setTemplate(templateFileName).getStatusLine.getStatusCode shouldBe 200
    esClient.deleteTemplate("template_example").getStatusLine.getStatusCode shouldBe 200
  }

}

