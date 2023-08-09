package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.enriched.EnrichedGenes
import bio.ferlab.datalake.spark3.testmodels.prepared.PreparedGeneCentric
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame


class GeneCentricSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  val data: Map[String, DataFrame] = Map(
    enriched_genes.id -> Seq(
      EnrichedGenes()
    ).toDF
  )

  "transformSingle" should "return data in expected format" in {

    val df = new GeneCentric(TestETLContext()).transformSingle(data)

    val result = df.as[PreparedGeneCentric].collect()
    result.length shouldBe 1
    result.head shouldBe PreparedGeneCentric()


  }

}
