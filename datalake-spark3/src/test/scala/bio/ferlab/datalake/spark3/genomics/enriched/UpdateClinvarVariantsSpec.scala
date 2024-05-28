package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.enriched.EnrichedVariant
import bio.ferlab.datalake.testutils.models.enriched.EnrichedVariant.CLINVAR
import bio.ferlab.datalake.testutils.models.normalized._
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.apache.spark.sql.DataFrame

class UpdateClinvarVariantsSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._


  val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")
  val variants: DatasetConf = conf.getDataset("enriched_variants")

  val clinvarDf: DataFrame = Seq(
    NormalizedClinvar(chromosome = "1", start = 69897, reference = "T", alternate = "C", clin_sig = List("Pathogenic")),
    NormalizedClinvar(chromosome = "2", start = 69897, reference = "T", alternate = "C", clin_sig = List("Begnin"))
  ).toDF
  val variantsDf: DataFrame = Seq(
    EnrichedVariant(), //clinvar updated from begnin to pathogenic
    EnrichedVariant(chromosome = "2", clinvar = null), // clinvar initially null, should be updated
    EnrichedVariant(chromosome = "3"), // clinvar previously set, now should be null

  ).toDF

  val etl = UpdateClinvarVariants(TestETLContext())
  private val data = Map(
    variants.id -> variantsDf,
    clinvar.id -> clinvarDf
  )

  "transformSingle" should "return expected result" in {
    val df = etl.transformSingle(data)
    val result = df.as[EnrichedVariant].collect()
    result.length shouldBe 3
    result should contain theSameElementsAs Seq(
      EnrichedVariant(clinvar = CLINVAR(clin_sig = List("Pathogenic"))),
      EnrichedVariant(chromosome="2", clinvar = CLINVAR(clin_sig = List("Begnin"))),
      EnrichedVariant(chromosome="3", clinvar = null)

    )
  }

}
