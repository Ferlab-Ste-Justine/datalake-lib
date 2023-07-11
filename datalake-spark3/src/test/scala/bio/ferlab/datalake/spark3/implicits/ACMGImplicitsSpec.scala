package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.ACMGImplicits._
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ACMGImplicitsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  spark.sparkContext.setLogLevel("ERROR")

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

}
