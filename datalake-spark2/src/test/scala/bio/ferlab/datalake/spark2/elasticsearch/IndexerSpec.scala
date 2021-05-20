package bio.ferlab.datalake.spark2.elasticsearch

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

case class TestDocs(id: String,
                    value: String,
                    value_2: String)

class IndexerSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("es.nodes", "localhost")
    .config("es.port","9200")
    .config("es.index.auto.create", "true")
    .config("spark.es.nodes.wan.only","true")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val indexName = "test"

  "indexer" should "push data to es" in {

    val df = spark.read.json(this.getClass.getResource("/data.json").getFile)

    df.saveToEs(s"$indexName/_doc", Map("es.mapping.id" -> "id"))
    spark.read.format("es").load("test").as[TestDocs].collect() should contain theSameElementsAs Seq(
      TestDocs("id1", "v", "1"),
      TestDocs("id2", "v", "2")
    )

  }

  "indexer" should "update data to es" in {
    val updateDF = spark.read.json(this.getClass.getResource("/update.json").getFile)

    updateDF.saveToEs(s"$indexName/_doc", Map("es.mapping.id" -> "id", "es.write.operation"-> "upsert"))

    spark.read.format("es").load("test").as[TestDocs].collect() should contain theSameElementsAs Seq(
      TestDocs("id1", "v", "1"),
      TestDocs("id2", "v", "3")
    )
  }

}

