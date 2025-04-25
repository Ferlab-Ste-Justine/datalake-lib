package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits
import bio.ferlab.datalake.spark3.publictables.normalized.gnomad.{GnomadConstraint, GnomadV4CNV}
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.normalized.{NormalizedGnomadConstraint, NormalizedGnomadV4CNV}
import bio.ferlab.datalake.testutils.models.raw.RawGnomadV4CNV
import bio.ferlab.datalake.testutils.{ClassGenerator, SparkSpec, TestETLContext}

class GnomadV4CNVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_gnomad_cnv_v4")
  val destination: DatasetConf = conf.getDataset("normalized_gnomad_cnv_v4")

  "transform" should "transform RawGnomadV4CNV to NormalizedGnomadV4CNV" in {
    val inputData = Map(source.id -> Seq(RawGnomadV4CNV()).toDF())

    val resultDF = new GnomadV4CNV(TestETLContext()).transformSingle(inputData)

    val expectedResults = Seq(NormalizedGnomadV4CNV())
    resultDF.as[NormalizedGnomadV4CNV].collect() shouldBe expectedResults
  }

}
