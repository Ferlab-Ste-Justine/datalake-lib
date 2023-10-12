package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.normalized.NormalizedOrphanetGeneSet
import bio.ferlab.datalake.testutils.models.raw.{RawOrphanetProduct6, RawOrphanetProduct9}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import java.io.File

/*
//TODO fixme
21/09/02 08:35:48 ERROR CodeGenerator: failed to compile: org.codehaus.commons.compiler.CompileException: File 'generated.java', Line 85, Column 27: Unexpected selector 'public' after "."
org.codehaus.commons.compiler.CompileException: File 'generated.java', Line 85, Column 27: Unexpected selector 'public' after "."

class OrphanetGeneSetSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers with BeforeAndAfterAll {

  import spark.implicits._

  val destination: DatasetConf = conf.getDataset("normalized_orphanet_gene_set")
  val orphanet_gene_association: DatasetConf  = conf.getDataset("raw_orphanet_gene_association")
  val orphanet_disease_history: DatasetConf  = conf.getDataset("raw_orphanet_disease_history")

  val job = new OrphanetGeneSet()

  override def beforeAll(): Unit = {
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${destination.table.map(_.database).getOrElse("variant")}")
      new File(destination.location).delete()
    }
  }

  "extract" should "return xml files parsed into a dataframes" in {

    val extractedData      = new OrphanetGeneSet().extract()
    val gene_associationDF = extractedData(orphanet_gene_association.id)
    val disease_historyDF  = extractedData(orphanet_disease_history.id)

    val expectedProduct6 = OrphanetProduct6()
    gene_associationDF.show(false)
    gene_associationDF.count shouldBe 3
    gene_associationDF
      .where($"orpha_code" === 447)
      .as[OrphanetProduct6]
      .collect()
      .head shouldBe expectedProduct6

    val expectedProduct9 = OrphanetProduct9()
    disease_historyDF.show(false)
    disease_historyDF.count shouldBe 2
    disease_historyDF
      .where($"orpha_code" === 58)
      .as[OrphanetProduct9]
      .collect()
      .head shouldBe expectedProduct9
  }

  "transform" should "return a joined data from product 6 and 9 files" in {

    val extractedData = job.extract()
    val outputDF      = job.transform(extractedData)

    outputDF.show(false)
    outputDF
      .where($"orpha_code" === 166024)
      .as[OrphanetGeneSetOutput]
      .collect()
      .head shouldBe OrphanetGeneSetOutput()

    outputDF.as[OrphanetGeneSetOutput].count() shouldBe 3

  }

  "load" should "create a hive table" in {

    spark.sql("CREATE DATABASE IF NOT EXISTS variant")

    job.run()
    val resultDF = spark.table(s"${job.destination.table.get.fullName}")

    resultDF.show(false)
    resultDF
      .where($"orpha_code" === 166024)
      .as[OrphanetGeneSetOutput]
      .collect()
      .head shouldBe OrphanetGeneSetOutput()

    resultDF.as[OrphanetGeneSetOutput].count() shouldBe 3

  }

}
*/

