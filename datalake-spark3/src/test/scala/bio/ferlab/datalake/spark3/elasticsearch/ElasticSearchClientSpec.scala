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

  "createIndex" should "create a new index" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      noException should be thrownBy client.createIndex("new_index")
      httpClient.send(basicRequest.get(uri"$url/new_index")).isSuccess shouldBe true
    }
  }

  it should "be idempotent when the index already exists" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.createIndex("existing_index")
      // Second call should not throw
      noException should be thrownBy client.createIndex("existing_index")
    }
  }

  it should "throw an IllegalStateException on unexpected errors" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(s"$url/fake_url")
      val ex = the[IllegalStateException] thrownBy client.createIndex("some_index")
      ex.getMessage should include("Server could not create index some_index")
    }
  }

  "getNumberOfReplicas" should "return the current replica count of an existing index" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      // Create index with explicit replica count
      httpClient.send(
        basicRequest.put(uri"$url/idx_replicas")
          .body("""{"settings": {"index": {"number_of_replicas": "2"}}}""")
          .contentType("application/json")
      ).isSuccess shouldBe true

      client.getNumberOfReplicas("idx_replicas") shouldBe Some("2")
    }
  }

  it should "return None when the index does not exist" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.getNumberOfReplicas("does_not_exist") shouldBe None
    }
  }

  it should "return None when the request fails (bad url)" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(s"$url/fake_url")
      client.getNumberOfReplicas("any_index") shouldBe None
    }
  }

  "setIndexSettings" should "update settings on an existing index" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.createIndex("idx_settings")

      noException should be thrownBy client.setIndexSettings(
        "idx_settings",
        """{"index": {"number_of_replicas": "0"}}"""
      )
      client.getNumberOfReplicas("idx_settings") shouldBe Some("0")
    }
  }

  it should "throw an IllegalStateException when the index does not exist" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      val ex = the[IllegalStateException] thrownBy client.setIndexSettings(
        "missing_index",
        """{"index": {"number_of_replicas": "0"}}"""
      )
      ex.getMessage should include("Server could not update settings for index missing_index")
    }
  }

  "replicas read/restore roundtrip" should "preserve the original replica count" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      // Create index with replica count 2
      httpClient.send(
        basicRequest.put(uri"$url/idx_roundtrip")
          .body("""{"settings": {"index": {"number_of_replicas": "2"}}}""")
          .contentType("application/json")
      ).isSuccess shouldBe true

      val original = client.getNumberOfReplicas("idx_roundtrip")
      original shouldBe Some("2")

      // Disable replicas
      client.setIndexSettings("idx_roundtrip", """{"index": {"number_of_replicas": "0"}}""")
      client.getNumberOfReplicas("idx_roundtrip") shouldBe Some("0")

      // Restore
      original.foreach(r => client.setIndexSettings("idx_roundtrip", s"""{"index": {"number_of_replicas": "$r"}}"""))
      client.getNumberOfReplicas("idx_roundtrip") shouldBe Some("2")
    }
  }

  "refreshIndex" should "refresh an existing index" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.createIndex("idx_refresh")
      noException should be thrownBy client.refreshIndex("idx_refresh")
    }
  }

  it should "throw an IllegalStateException when the index does not exist" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      val ex = the[IllegalStateException] thrownBy client.refreshIndex("missing_index")
      ex.getMessage should include("Server could not refresh index missing_index")
    }
  }

  "forceMergeIndex" should "force merge an existing index" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.createIndex("idx_merge")
      noException should be thrownBy client.forceMergeIndex("idx_merge")
    }
  }

  it should "accept a custom max_num_segments" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.createIndex("idx_merge_custom")
      noException should be thrownBy client.forceMergeIndex("idx_merge_custom", maxNumSegments = 2)
    }
  }

  it should "not throw when the index does not exist (best-effort)" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      // Force merge is optimization-only — a missing index should log a warning, not crash the pipeline.
      noException should be thrownBy client.forceMergeIndex("missing_index")
    }
  }

  it should "not throw on read timeout (merge continues on ES in background)" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(url)
      client.createIndex("idx_merge_timeout")
      // 1ms timeout guarantees the HTTP call will not complete in time
      noException should be thrownBy client.forceMergeIndex(
        "idx_merge_timeout",
        readTimeout = scala.concurrent.duration.Duration(1, "millisecond")
      )
    }
  }

  it should "not throw when the server is unreachable (best-effort)" in {
    withESContainer { url =>
      val client = new ElasticSearchClient(s"$url/fake_url")
      noException should be thrownBy client.forceMergeIndex("any_index")
    }
  }
}
