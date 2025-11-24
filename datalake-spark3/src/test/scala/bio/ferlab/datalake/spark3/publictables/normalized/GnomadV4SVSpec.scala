package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.publictables.normalized.gnomad.GnomadV4SV
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.normalized.NormalizedGnomadV4SV
import bio.ferlab.datalake.testutils.models.raw.RawGnomadV4SV
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class GnomadV4SVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_gnomad_sv_v4")
  val destination: DatasetConf = conf.getDataset("normalized_gnomad_sv_v4")

  "transform" should "transform RawGnomadV4SV to NormalizedGnomadV4SV" in {
    val inputData = Map(source.id -> Seq(RawGnomadV4SV()).toDF())

    val resultDF = new GnomadV4SV(TestETLContext()).transformSingle(inputData)

    val expectedResults = Seq(NormalizedGnomadV4SV())
    resultDF.as[NormalizedGnomadV4SV].collect() shouldBe expectedResults
  }

}
