package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.etl.v3.TestETLContext
import bio.ferlab.datalake.spark3.testmodels.normalized.{NormalizedClinvar, NormalizedCosmic}
import bio.ferlab.datalake.spark3.testmodels.raw.{RawClinvar, RawCosmic}
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{ClassGenerator, WithSparkSession}
import io.delta.tables.DeltaTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.io.File
import scala.util.Try

class CosmicGeneSetSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with WithTestConfig with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_cosmic_gene_set")
  val destination: DatasetConf = conf.getDataset("normalized_cosmic_gene_set")

  override def beforeAll(): Unit = {
    Try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${destination.table.map(_.database).getOrElse("variant")}")
      new File(destination.location).delete()
    }
  }

  "transform" should "transform Cosmic input to Cosmic output" in {
    val df = Seq(RawCosmic()).toDF()

    val result = new CosmicGeneSet(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedCosmic].collect() should contain theSameElementsAs Seq(NormalizedCosmic())

  }


}



