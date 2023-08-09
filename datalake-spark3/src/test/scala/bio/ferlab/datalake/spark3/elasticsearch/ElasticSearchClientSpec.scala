package bio.ferlab.datalake.spark3.elasticsearch

import bio.ferlab.datalake.testutils.SparkSpec
import com.dimafeng.testcontainers.ElasticsearchContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.testcontainers.utility.DockerImageName
import sttp.client3.{SimpleHttpClient, UriContext, basicRequest}
import sttp.model.StatusCode.NotFound

class ElasticSearchClientSpec extends SparkSpec with TestContainerForEach with BeforeAndAfterAll {
  override val containerDef: ElasticsearchContainer.Def = ElasticsearchContainer.Def(DockerImageName
    .parse("elasticsearch:7.17.10")
    .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch")
  )


  private val httpClient = SimpleHttpClient()

  def withESContainer(f: String => Unit): Unit = withContainers { elasticsearchContainer =>
    f(s"http://${elasticsearchContainer.httpHostAddress}")
  }

  "isRunning" should "return true" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.isRunning shouldBe true
    }
  }

  it should "return false" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(s"$url/fake_url")
      client.isRunning shouldBe false
    }
  }

  "checkNodeRoles" should "return true" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.checkNodeRoles shouldBe true
    }
  }

  "deleteIndex" should "return true even if the index does not exist" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.deleteIndex("not_existing_index")
    }
  }
  it should "delete an existing index" in {
    withESContainer { url =>
      httpClient.send(basicRequest.put(uri"$url/test_index").body("{}").contentType("application/json")).isSuccess shouldBe true
      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.deleteIndex("test_index")
      httpClient.send(basicRequest.get(uri"$url/test_index")).code shouldBe NotFound
    }
  }

  it should "throw an IllegalStateException" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(s"$url/fake_url")
      the [IllegalStateException] thrownBy {
        client.deleteIndex("not_existing_index")
      } should have message """Server could not delete index and replied :{"error":"Incorrect HTTP method for uri [/fake_url/not_existing_index?ignore_unavailable=true] and method [DELETE], allowed: [POST]","status":405}"""

    }
  }

  "setTemplate" should "create a template from resource" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.setTemplate("utils/template1.json")
      httpClient.send(basicRequest.get(uri"$url/_index_template/template1")).isSuccess shouldBe true
    }
  }

  it  should "create a template from a file" in {
    withESContainer { url =>
      val file = getClass.getResource("/utils/template1.json").getFile
      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.setTemplate(file)
      httpClient.send(basicRequest.get(uri"$url/_index_template/template1")).isSuccess shouldBe true
    }
  }

  it should "thrown an exception if file is malformed" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      the [IllegalStateException] thrownBy {
        client.setTemplate("utils/template_malformed.json")
      } should have message """Server could not set template and replied :400 : {"error":{"root_cause":[{"type":"x_content_parse_exception","reason":"[6:3] [index_template] unknown field [malformed]"}],"type":"x_content_parse_exception","reason":"[6:3] [index_template] unknown field [malformed]"},"status":400}"""
    }
  }

  "setAlias" should "create an alias" in {
    withESContainer { url =>
      //Create 3 indices
      httpClient.send(basicRequest.put(uri"$url/test_index_1").body("{}").contentType("application/json")).isSuccess shouldBe true
      httpClient.send(basicRequest.put(uri"$url/test_index_2").body("{}").contentType("application/json")).isSuccess shouldBe true
      httpClient.send(basicRequest.put(uri"$url/test_index_3").body("{}").contentType("application/json")).isSuccess shouldBe true

      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.setAlias(List("test_index_1", "test_index_2"), Nil, "test_alias")
      httpClient.send(basicRequest.get(uri"$url/test_alias")).isSuccess shouldBe true
      val body1 = httpClient.send(basicRequest.get(uri"$url/test_alias/_alias")).body.right.get
      assert(body1.contains("test_index_1"))
      assert(body1.contains("test_index_2"))

      noException should be thrownBy client.setAlias(List("test_index_3"), List("test_index_1"), "test_alias")
      val body2 = httpClient.send(basicRequest.get(uri"$url/test_alias/_alias")).body.right.get
      assert(!body2.contains("test_index_1"))
      assert(body2.contains("test_index_2"))
      assert(body2.contains("test_index_3"))

    }
  }

  "getAliasIndices" should "return list of indices contained in a given alias" in {
    withESContainer { url =>
      //Create 3 indices
      httpClient.send(basicRequest.put(uri"$url/test_index_1").body("{}").contentType("application/json")).isSuccess shouldBe true
      httpClient.send(basicRequest.put(uri"$url/test_index_2").body("{}").contentType("application/json")).isSuccess shouldBe true
      httpClient.send(basicRequest.put(uri"$url/test_index_3").body("{}").contentType("application/json")).isSuccess shouldBe true

      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.setAlias(List("test_index_1", "test_index_2"), Nil, "test_alias")

      val indices = client.getAliasIndices("test_alias")

      indices should contain theSameElementsAs Seq("test_index_1", "test_index_2")

    }
  }

  it should "return empty set if alias does not exist" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.getAliasIndices("unknown_alias")
      client.getAliasIndices("unknown_alias") shouldBe empty

    }
  }
}
