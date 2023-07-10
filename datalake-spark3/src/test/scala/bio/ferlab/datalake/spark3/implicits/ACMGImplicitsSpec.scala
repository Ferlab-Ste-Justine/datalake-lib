package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.ACMGImplicits._
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ACMGImplicitsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  "get_BA1" should "throw IllegalArgumentException if `external_frequencies` column is absent" in {
    val structureData = Seq(
      Row(1), Row(2)
    )

    val structureSchema = new StructType()
      .add("start", IntegerType, true)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema)

    an[IllegalArgumentException] should be thrownBy df.get_BA1
  }

  it should "return the correct BA1 classification" in {

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
        .add("study", StringType, true)
        .add("max_af", DoubleType, true)
        .add("score", BooleanType, true))

    val resultData = Seq(
      Row(Row("topmed_bravo", 0.050, true)),
      Row(Row("thousand_genomes", 0.010, false))
    )

    val queryDF = spark.createDataFrame(
      spark.sparkContext.parallelize(queryData), querySchema)

    val resultDF = spark.createDataFrame(
      spark.sparkContext.parallelize(resultData), resultSchema)

    val result = queryDF.withColumn("BA1", queryDF.get_BA1).select("BA1")

    resultDF.collect().sameElements(result.collect()) shouldEqual true
  }

}
