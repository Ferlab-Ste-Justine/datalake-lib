package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.ACMGImplicits._
import bio.ferlab.datalake.testutils.WithSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ACMGImplicitsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  spark.sparkContext.setLogLevel("ERROR")

  val variantSchema = new StructType()
    .add("chromosome", StringType, true)
    .add("start", IntegerType, true)
    .add("end", IntegerType, true)
    .add("reference", StringType, true)
    .add("alternate", StringType, true)

  def ba1Fixture = {
    new {
      val querySchema = new StructType()
        .add("start", IntegerType, true)
        .add("external_frequencies", new StructType()
          .add("thousand_genomes", new StructType()
            .add("af", DoubleType, true)
            .add("an", IntegerType, true))
          .add("topmed_bravo", new StructType()
            .add("af", DoubleType, true)
            .add("an", IntegerType, true)))

      val queryData = Seq(
        Row(1, Row(Row(0.001, 2), Row(0.050, 50))),
        Row(2, Row(Row(0.010, 12), Row(0.001, 3))),
      )

      val resultSchema = new StructType()
        .add("BA1", new StructType()
          .add("cohort", StringType, false)
          .add("max_af", DoubleType, true)
          .add("score", BooleanType, true),
          false)

      val resultData = Seq(
        Row(Row("topmed_bravo", 0.050, true)),
        Row(Row("thousand_genomes", 0.010, false))
      )

      val queryDF = spark.createDataFrame(spark.sparkContext.parallelize(queryData), querySchema)
      val result = queryDF.withColumn("BA1", queryDF.getBA1()).select("BA1")
    }
  }

  "get_BA1" should "throw IllegalArgumentException if `external_frequencies` column is absent" in {
    val structureData = Seq(Row(1), Row(2))
    val structureSchema = new StructType().add("start", IntegerType, true)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)

    an[IllegalArgumentException] should be thrownBy df.getBA1()
  }

  it should "return the correct BA1 schema" in {
    val f = ba1Fixture
    f.result.schema shouldBe f.resultSchema
  }

  it should "return the correct BA1 classification data" in {
    val f = ba1Fixture
    f.result.collect() should contain theSameElementsAs f.resultData
  }

  def bs2Fixture = {
    new {
      val orphanetSchema = new StructType()
        .add("gene_symbol", StringType, false)
        .add("average_age_of_onset", new ArrayType(StringType, true), true)
        .add("type_of_inheritance", new ArrayType(StringType, true), true)

      val orphanetData = Seq(
        Row("gene1", Array("Neonatal", "Antenatal"), Array("Autosomal recessive")),
        Row("gene2", Array("Neonatal"), Array("Autosomal dominant")),
        Row("gene3", Array("All ages"), Array("Autosomal dominant")),
      )

      val orphanetDF = spark.createDataFrame(spark.sparkContext.parallelize(orphanetData), orphanetSchema)

      val freqSchema = variantSchema
        .add("genes_symbol", new ArrayType(StringType, true), true)
        .add("external_frequencies", new StructType()
          .add("gnomad_genomes_3_1_1", new StructType()
            .add("ac", IntegerType, true)
            .add("hom", IntegerType, true)))

      val freqData = Seq(Row("1", 1, 2, "A", "C", Array("gene1"), Row(Row(10, 0))))

      val freqDF = spark.createDataFrame(spark.sparkContext.parallelize(freqData), freqSchema)

      val querySchema = variantSchema
        .add("symbol", StringType, true)

      val queryData = Seq(Row("1", 1, 2, "A", "C", "gene1"))

      val queryDF = spark.createDataFrame(spark.sparkContext.parallelize(queryData), querySchema)
    }
  }

  "get_BS2" should "throw IllegalArgumentException if `average_age_of_onset` column is absent from the Orphanet DataFrame" in {
    val f = bs2Fixture

    an[IllegalArgumentException] should be thrownBy f.queryDF.getBS2(f.orphanetDF.drop("average_age_of_onset"), f.freqDF)
  }

  it should "return observed homozygote alleles as BS2 true" in {
    val f = bs2Fixture

    val freqData = Seq(
      Row("1", 1, 2, "A", "C", Array("gene1"), Row(Row(25, 4))),
      Row("1", 3, 4, "T", "C", Array("gene1"), Row(Row(10, 0))))
    val freqDF = spark.createDataFrame(spark.sparkContext.parallelize(freqData), f.freqSchema)

    val queryData = Seq(
      Row("1", 1, 2, "A", "C", "gene1"),
      Row("1", 3, 4, "T", "C", "gene1"),
      Row("1", 5, 6, "G", "C", "gene1"))
    val queryDF = spark.createDataFrame(spark.sparkContext.parallelize(queryData), f.querySchema)

    val resultData = Seq(
      Row("1", 1, 2, "A", "C", "gene1", Row(25, 4, false, true)),
      Row("1", 3, 4, "T", "C", "gene1", Row(10, 0, false, false)),
      Row("1", 5, 6, "G", "C", "gene1", Row(null, null, false, false)),
    )

    val result = queryDF.getBS2(f.orphanetDF, freqDF)
    result.collect() should contain theSameElementsAs resultData
  }

  it should "return observed heterozygote allele in recessive non-adult onset diseases as BS2 true" in {
    val f = bs2Fixture

    val freqData = Seq(
      Row("1", 1, 2, "A", "C", Array("gene1"), Row(Row(15, 0))),
      Row("1", 1, 2, "A", "C", Array("gene2"), Row(Row(15, 0))),
      Row("1", 1, 2, "A", "C", Array("gene3"), Row(Row(15, 0))),
    )
    val freqDF = spark.createDataFrame(spark.sparkContext.parallelize(freqData), f.freqSchema)

    val queryData = Seq(
      Row("1", 1, 2, "A", "C", "gene1"),
      Row("1", 1, 2, "A", "C", "gene2"),
      Row("1", 1, 2, "A", "C", "gene3"))
    val queryDF = spark.createDataFrame(spark.sparkContext.parallelize(queryData), f.querySchema)

    val resultData = Seq(
      Row("1", 1, 2, "A", "C", "gene1", Row(15, 0, false, false)),
      Row("1", 1, 2, "A", "C", "gene2", Row(15, 0, true, true)),
      Row("1", 1, 2, "A", "C", "gene3", Row(15, 0, false, false)),
    )

    val result = queryDF.getBS2(f.orphanetDF, freqDF)
    result.collect() should contain theSameElementsAs resultData
  }

  def pp2Fixture = {
    new {
      val clinvarSchema = new StructType()
        .add("geneinfo", StringType, true)
        .add("clin_sig", new ArrayType(StringType, true), true)
        .add("mc", new ArrayType(StringType, true), true)

      val clinvarData = Seq(
        Row("gene1", Array("Pathogenic"), Array("missense_variant")),
        Row("gene1", Array("Pathogenic"), Array("missense_variant")),
        Row("gene1", Array("Pathogenic"), Array("missense_variant")),
        Row("gene1", Array("Benign"), Array("missense_variant")),
        Row("gene1", Array("Pathogenic"), Array("upstream_gene_variant")),
        Row("gene2", Array("Benign"), Array("missense_variant")),
      )

      val clinvarDF = spark.createDataFrame(spark.sparkContext.parallelize(clinvarData), clinvarSchema)

      val querySchema = variantSchema
        .add("symbol", StringType, true)
        .add("consequences", new ArrayType(StringType, true), true)

      val queryData = Seq(
        Row("1", 1, 2, "A", "C", "gene1", Array("missense_variant")),
        Row("1", 1, 2, "A", "T", "gene1", Array("upstream_gene_variant")),
        Row("1", 1, 2, "A", "T", "gene2", Array("missense_variant"))
      )

      val queryDF = spark.createDataFrame(spark.sparkContext.parallelize(queryData), querySchema)

      val resultSchema = variantSchema
        .add("symbol", StringType, true)
        .add("consequences", new ArrayType(StringType, true), true)
        .add("pp2", new StructType()
          .add("n_benign", IntegerType, false)
          .add("n_pathogenic", IntegerType, false)
          .add("is_missense_pathogenic", BooleanType, false)
          .add("score", BooleanType, true), false
        )

      val resultData = Seq(
        Row("1", 1, 2, "A", "C", "gene1", Array("missense_variant"), Row(1, 3, true, true)),
        Row("1", 1, 2, "A", "T", "gene1", Array("upstream_gene_variant"), Row(1, 3, true, false)),
        Row("1", 1, 2, "A", "T", "gene2", Array("missense_variant"), Row(1, 0, false, false)),
      )

      val resultDF = spark.createDataFrame(spark.sparkContext.parallelize(resultData), resultSchema)

    }
  }

  "get_PP2" should "throw IllegalArgumentException if `mc` column is absent from the clinvar DataFrame" in {
    val f = pp2Fixture

    an[IllegalArgumentException] should be thrownBy f.queryDF.getPP2(f.clinvarDF.drop("mc"))
  }

  it should "correctly classify PP2 variants" in {
    val f = pp2Fixture

    val result = f.queryDF.getPP2(f.clinvarDF)
    result.collect() should contain theSameElementsAs f.resultDF.collect()
  }


}
