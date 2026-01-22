package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.publictables.normalized.omim.OmimGeneSet
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.models.normalized.{NormalizedOmimGeneSet, PHENOTYPE}
import bio.ferlab.datalake.testutils.models.raw.RawOmimGeneSet
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}

class OmimGeneSetSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_omim_gene_set")
  val destination: DatasetConf = conf.getDataset("normalized_omim_gene_set")

  "transform" should "transform RawOmimGeneSet to NormalizedOmimGeneSet" in {

    val rawOmimGeneSet2PhenotypeName = "Acute myeloid leukemia, somatic"
    val rawOmimGeneSet3PhenotypeName = "Hemolytic anemia due to phosphofructokinase deficiency"

    val inputData = Map(source.id -> Seq(RawOmimGeneSet(), // with phenotypes having omim_id and inheritance
      RawOmimGeneSet(_c12 = rawOmimGeneSet2PhenotypeName + ", 601626 (3)"), // with phenotypes having omim_id and no inheritance
      RawOmimGeneSet(_c12 = rawOmimGeneSet3PhenotypeName + " (1)")  // with phenotypes having no omim_id and no inheritance
    ).toDF())

    val resultDF = new OmimGeneSet(TestETLContext()).transformSingle(inputData)

    val expectedResults = Seq(NormalizedOmimGeneSet(),
      NormalizedOmimGeneSet(phenotype = PHENOTYPE(name = rawOmimGeneSet2PhenotypeName,
        omim_id = "601626", null, null)),
      NormalizedOmimGeneSet(phenotype = PHENOTYPE(name = rawOmimGeneSet3PhenotypeName,
        omim_id = null, null, null))
    )
    resultDF.as[NormalizedOmimGeneSet].collect() shouldBe expectedResults
  }
}

