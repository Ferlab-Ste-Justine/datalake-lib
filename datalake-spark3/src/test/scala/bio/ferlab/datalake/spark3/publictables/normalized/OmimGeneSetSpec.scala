package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.testmodels.normalized.NormalizedOmimGeneSet
import bio.ferlab.datalake.spark3.testmodels.raw.RawOmimGeneSet
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.WithSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.io.File
import scala.util.Try

class OmimGeneSetSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_omim_gene_set")
  val destination: DatasetConf = conf.getDataset("normalized_omim_gene_set")

  override def beforeAll(): Unit = {
    Try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${destination.table.map(_.database).getOrElse("variant")}")
      new File(destination.location).delete()
    }
  }

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

