package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.Format.{DELTA, VCF}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatalakeConf, DatasetConf, SimpleConfiguration, StorageConf, TableConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import bio.ferlab.datalake.testutils.{TestETLContext, WithSparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

class ClinvarVepSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers with BeforeAndAfterAll {

  val srcConf: DatasetConf = DatasetConf("raw_clinvar_vep", "raw", "/clinvar.vep.vcf", VCF, OverWrite, Some(TableConf("raw_db", "raw_airports")), readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true"))
  val destConf: DatasetConf = DatasetConf("normalized_clinvar_vep", "normalized", "/clinvar_vep", DELTA, OverWrite, Some(TableConf("raw_db", "raw_airports")), readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true"))
  implicit val conf: SimpleConfiguration = SimpleConfiguration(DatalakeConf(storages = List(
    StorageConf("raw", getClass.getClassLoader.getResource("input_vcf").getFile, LOCAL),
    StorageConf("normalized", getClass.getClassLoader.getResource("normalized/").getFile, LOCAL)),
    sources = List(srcConf, destConf)
  ))

  "ClinvarVepSpec" should "run" in {

    val data = ClinvarVep(TestETLContext()).extract()
    val df = ClinvarVep(TestETLContext()).transformSingle(data)

    df.toJSON.collect.foreach(println)

    1 shouldBe 0
  }

}