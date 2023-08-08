package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

class OmimGeneSetSpec extends SparkSpec with WithTestConfig with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  val source: DatasetConf = conf.getDataset("raw_omim_gene_set")
  val destination: DatasetConf = conf.getDataset("normalized_omim_gene_set")

  override val dbToCreate: List[String] = List(destination.table.map(_.database).getOrElse("variant"))
  override val dsToClean: List[DatasetConf] = List(destination)

  /*
  //TODO fix this
  ANTLR Tool version 4.7 used for code generation does not match the current runtime version 4.8

  "ImportOmimGeneSet" should "transform data into expected format" in {

    val inputDf  = Map(source.id -> Seq(OmimGeneSetInput()).toDF())
    val outputDf = new OmimGeneSet().transform(inputDf)

    outputDf.as[OmimGeneSetOutput].collect() should contain theSameElementsAs Seq(OmimGeneSetOutput())
  }
  */
}

