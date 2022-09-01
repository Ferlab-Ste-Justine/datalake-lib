package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.testmodels.Genotype
import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.ParentalOrigin._

class GenomicImplicitsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  val wtDf: DataFrame = Seq(Genotype(Array(0, 0)), Genotype(Array(-1, -1)), Genotype(Array(0))).toDF()
  val homDf: DataFrame = Seq(Genotype(Array(1, 1))).toDF()
  val hetDf: DataFrame = Seq(Genotype(Array(0, 1)), Genotype(Array(1, 0)), Genotype(Array(-1, 1)), Genotype(Array(1, -1))).toDF()
  val hemDf: DataFrame = Seq(Genotype(Array(1))).toDF()
  val unkDf: DataFrame = Seq(Genotype(Array(-1, 0))).toDF()

  "zygosity" should "return HOM for 1/1" in {
    homDf.select(zygosity($"calls")).as[String].collect() should contain only "HOM"
  }
  it should "return HET for 0/1" in {
    hetDf.select(zygosity($"calls")).as[String].collect() should contain only "HET"
  }
  it should "return WT for 0/0" in {
    wtDf.select(zygosity($"calls")).as[String].collect() should contain only "WT"
  }
  it should "return HEM for array(1)" in {
    hemDf.select(zygosity($"calls")).as[String].collect() should contain only "HEM"
  }

  it should "return UNK otherwise" in {
    unkDf.select(zygosity($"calls")).as[String].collect() should contain only "UNK"
  }

  "colFromArrayOrField" should "return first element of an array" in {
    val df = Seq(
      Seq(1)
    ).toDF("first_array")

    val res = df.select(SparkUtils.colFromArrayOrField(df, "first_array") as "first_col")
    res.as[Int].collect() should contain only 1

    val df2 = Seq(
      (Seq(1), Seq(2)),
    ).toDF("first_array", "sec_array")

    val res2 = df2.select(SparkUtils.colFromArrayOrField(df2, "first_array") as "first_col")
    res2.as[Int].collect() should contain only 1
  }

  it should "return column" in {
    val df = Seq(
      1
    ).toDF("first_col")

    val res = df.select(SparkUtils.colFromArrayOrField(df, "first_col"))
    res.as[Int].collect() should contain only 1

    val df2 = Seq(
      (1, 2),
    ).toDF("first_col", "sec_col")

    val res2 = df2.select(SparkUtils.colFromArrayOrField(df2, "first_col"))
    res2.as[Int].collect() should contain only 1
  }

  it should "return parental origins accordingly" in {
    val input_occurrences = Seq(
      ("1", true, Array(0, 1), Array(0, 0), Array(0, 0), "HET", DENOVO),
      ("1", true, Array(0, 1), Array(0, 0), Array(0, 1), "HET", MTH),
      ("1", true, Array(0, 1), Array(0, 0), Array(1, 0), "HET", MTH),
      ("1", true, Array(0, 1), Array(0, 0), Array(1, 1), "HET", MTH),
      ("1", true, Array(0, 1), Array(0, 0), Array(-1, 1), "HET", null),
      //hemizygote Array(0) is NOT considered as Array(0, 0)
      ("1", true, Array(0, 1), Array(0), Array(-1, 1), "HET", null),

      ("1", true, Array(0, 1), Array(0, 1), Array(0, 0), "HET", FTH),
      ("1", true, Array(0, 1), Array(0, 1), Array(0, 1), "HET", AMBIGUOUS),
      ("1", true, Array(0, 1), Array(0, 1), Array(1, 0), "HET", AMBIGUOUS),
      ("1", true, Array(0, 1), Array(0, 1), Array(1, 1), "HET", MTH),
      ("1", true, Array(0, 1), Array(0, 1), Array(-1, 1), "HET", null),

      ("1", true, Array(0, 1), Array(1, 0), Array(0, 0), "HET", FTH),
      ("1", true, Array(0, 1), Array(1, 0), Array(0, 1), "HET", AMBIGUOUS),
      ("1", true, Array(0, 1), Array(1, 0), Array(1, 0), "HET", AMBIGUOUS),
      ("1", true, Array(0, 1), Array(1, 0), Array(1, 1), "HET", MTH),
      ("1", true, Array(0, 1), Array(1, 0), Array(-1, 1), "HET", null),

      ("1", true, Array(0, 1), Array(1, 1), Array(0, 0), "HET", FTH),
      ("1", true, Array(0, 1), Array(1, 1), Array(0, 1), "HET", FTH),
      ("1", true, Array(0, 1), Array(1, 1), Array(1, 0), "HET", FTH),
      ("1", true, Array(0, 1), Array(1, 1), Array(1, 1), "HET", AMBIGUOUS),
      ("1", true, Array(0, 1), Array(1, 1), Array(-1, -1), "HET", FTH),
      //hemizygote Array(1) is NOT considered as Array(0, 0)
      ("1", true, Array(0, 1), Array(1), Array(-1, 1), "HET", null),

      ("1", true, Array(0, 1), Array(-1, 1), Array(0, 0), "HET", null),
      ("1", true, Array(0, 1), Array(-1, 1), Array(0, 1), "HET", null),
      ("1", true, Array(0, 1), Array(-1, 1), Array(1, 0), "HET", null),
      ("1", true, Array(0, 1), Array(-1, 1), Array(-1, 1), "HET", null),

      ("1", true, Array(0, 1), Array(0, 1), Array(0, 0), "WT", null),
      ("1", true, Array(0, 1), Array(0, 1), null, "HET", POSSIBLE_FATHER),
      ("1", true, Array(0, 1), null, Array(0, 0), "HET", POSSIBLE_DENOVO),
      ("X", true, Array(0, 1), Array(1), Array(0, 0), "HET", FTH),
      //null should be transform to Array(-1,-1)
      ("Y", true, Array(1), Array(1), null, "HET", FTH),
    ).toDF("chromosome", "is_multi_allelic", "calls", "father_calls", "mother_calls", "zygosity", "expectedResult")

    val result = input_occurrences.withParentalOrigin("parental_origin", $"calls", $"father_calls", $"mother_calls")

    result.show(100, false)
    result
      .where(functions.not(col("expectedResult") === col("parental_origin")) or
        (col("expectedResult").isNotNull and col("parental_origin").isNull) or
        (col("expectedResult").isNull and col("parental_origin").isNotNull))
      .select("expectedResult", "parental_origin")
      .as[(String, String)]
      .collect() shouldBe Array.empty[(String, String)]
  }

  it should "return autosomal transmissions accordingly" in {

    val input_occurrences = List(

      // Les 100 combinaisons avec calls=0/0 (affected_status=true)
      ("1", "Male", false, Array(0, 0), Array(0, 0), Array(0, 0), true, true, true, "non_carrier_proband"), // Les 100 combinaisons avec calls=0/0

      // Les 100 combinaisons avec calls=0/1 (affected_status=true)
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(0, 0), true, false, false, "autosomal_dominant_de_novo"),
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(0, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(0, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(0, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(1, 0), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(1, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(1, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(1, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 0), Array(-1, -1), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(0, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(0, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(0, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(1, 0), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(1, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(-1, -1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(-1, -1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(0, 1), Array(-1, -1), true, false, true, null), // Les 2 autres combinaisons (1 booléen)
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(0, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(0, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(0, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(1, 0), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(1, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(-1, -1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(-1, -1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 0), Array(-1, -1), true, false, true, null), // Les 2 autres combinaisons (1 booléen)
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(0, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(0, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(0, 1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(1, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(1, 1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(-1, -1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(-1, -1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(1, 1), Array(1, 1), true, false, true, null), // Les 10 combinaisons avec calls=0/1 et father_calls=1/1 (unaffected father)
      ("1", "Male", false, Array(0, 1), Array(-1, -1), Array(0, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(-1, -1), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(-1, -1), Array(1, 0), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(-1, -1), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(-1, -1), Array(1, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(-1, -1), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(0, 1), Array(-1, -1), Array(0, 0), true, true, true, null), // Les 14 autres combinaisons avec calls=0/1 et father_calls=-1/-1

      // Les 100 combinaisons avec calls=0/1 (affected_status=false)
      //TODO("1", "Male", false, Array(0, 1),   Array(0, 0),   Array(0, 0),   false, true , true , null), // Les 100 combinaisons avec calls=0/1

      // Les 100 combinaisons avec calls=0/1 (affected_status=true)
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(0, 0), true, false, false, "autosomal_dominant_de_novo"),
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(0, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(0, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(0, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(1, 0), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(1, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(1, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(1, 1), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 0), Array(-1, -1), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(0, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(0, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(0, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(1, 0), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(1, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(-1, -1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(-1, -1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(0, 1), Array(-1, -1), true, false, true, null), // Les 2 autres combinaisons (1 booléen)
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(0, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(0, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(0, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(1, 0), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(1, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(-1, -1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(-1, -1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 0), Array(-1, -1), true, false, true, null), // Les 2 autres combinaisons (1 booléen)
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(0, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(0, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(0, 1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(1, 0), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(1, 1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(-1, -1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(-1, -1), true, true, false, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(1, 1), Array(1, 1), true, false, true, null), // Les 10 combinaisons avec calls=0/1 et father_calls=1/1 (unaffected father)
      ("1", "Male", false, Array(1, 0), Array(-1, -1), Array(0, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(-1, -1), Array(0, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(-1, -1), Array(1, 0), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(-1, -1), Array(1, 0), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(-1, -1), Array(1, 1), true, false, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(-1, -1), Array(1, 1), true, true, true, "autosomal_dominant"),
      ("1", "Male", false, Array(1, 0), Array(-1, -1), Array(0, 0), true, true, true, null), // Les 14 autres combinaisons avec calls=1/0 et father_calls=-1/-1

      // Les 100 combinaisons avec calls=1/0 (affected_status=false)
      //TODO("1", "Male", false, Array(1, 0),   Array(0, 0),   Array(0, 0),   false, true , true , null), // Les 100 combinaisons avec calls=1/0

      // Les 100 combinaisons avec calls=1/1 (affected_status=true)
      ("1", "Male", false, Array(1, 1), Array(0, 0), Array(1, 1), true, true, true, null), // Les 20 combinaisons avec calls=1/1 et father_calls=0/0
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(0, 0), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(0, 1), true, false, false, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(0, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(1, 0), true, false, false, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(1, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(1, 1), true, false, true, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(1, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(0, 1), Array(-1, -1), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(0, 0), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(0, 1), true, false, false, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(0, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(1, 0), true, false, false, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(1, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(1, 1), true, false, true, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(1, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 0), Array(-1, -1), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(0, 0), true, true, true, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(0, 1), true, true, false, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(0, 1), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(1, 0), true, true, false, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(1, 0), true, true, true, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(1, 1), true, true, true, "autosomal_recessive"),
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(1, 1), true, true, false, null), // Les 3 autres combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(1, 1), Array(-1, -1), true, true, false, null), // Les 4 combinaisons (2 booléens)
      ("1", "Male", false, Array(1, 1), Array(-1, -1), Array(1, 1), true, true, true, null), // Les 20 combinaisons avec calls=1/1 et father_calls=-1/-1

      // Les 100 combinaisons avec calls=1/1 (affected_status=false)
      //TODO("1", "Male", false, Array(1, 1),   Array(0, 0),   Array(0, 0),   false, true , true , null), // Les 100 combinaisons avec calls=1/1

      // Les 100 combinaisons avec calls=-1/-1 (affected_status=true)
      ("1", "Male", false, Array(-1, -1), Array(0, 0), Array(0, 0), true, true, true, "unknown_proband_genotype"), // Les 100 combinaisons avec calls=-1/-1

      // Les 100 combinaisons avec calls=-1/-1 (affected_status=false)
      //TODO("1", "Male", false, Array(-1, -1), Array(0, 0),   Array(0, 0),   false, true , true , null), // Les 100 combinaisons avec calls=-1/-1

      // Les cas où les données sont null
      ("1", "Male", false, Array(0, 0), Array(0, 0), null, true, true, true, "unknown_mother_genotype"),
      ("1", "Male", false, Array(0, 0), null, Array(0, 0), true, true, true, "unknown_father_genotype"),
      ("1", "Male", false, null, Array(0, 0), Array(0, 0), true, true, true, "unknown_proband_genotype"),
      ("1", null, false, Array(0, 0), Array(0, 0), Array(0, 0), true, true, true, "non_carrier_proband"),
    )
      .toDF("chromosome", "gender", "is_multi_allelic", "proband_calls", "father_calls", "mother_calls",
        "affected_status", "father_affected_status", "mother_affected_status", "expected_transmission")

    val result = input_occurrences
      .withGenotypeTransmission("transmission",
        "proband_calls",
        "gender",
        "affected_status",
        "father_calls",
        "father_affected_status",
        "mother_calls",
        "mother_affected_status")

    result.show(false)
    result
      .where(
        functions.not(col("expected_transmission") === col("transmission")) or
          (col("expected_transmission").isNotNull and col("transmission").isNull) or
          (col("expected_transmission").isNull and col("transmission").isNotNull))
      .select("expected_transmission", "transmission")
      .as[(String, String)]
      .collect() shouldBe Array.empty[(String, String)] //makes it easy to debug in case the test fails

  }

  it should "return sexual transmissions accordingly" in {

    val input_occurrences = List(

      // Les 64 combinaisons avec calls=0 (affected_status=true)
      ("X", "Male", false, Array(0), Array(0), Array(0, 0), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(0), Array(0, 1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(0), Array(1, 1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(0), Array(-1, -1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(1), Array(0, 0), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(1), Array(0, 1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(1), Array(1, 1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(1), Array(-1, -1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(-1), Array(0, 0), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(-1), Array(0, 1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(-1), Array(1, 1), true, true, true, "non_carrier_proband"),
      ("X", "Male", false, Array(0), Array(-1), Array(-1, -1), true, true, true, "non_carrier_proband"),

      ("X", "Female", false, Array(0, 0), Array(0), Array(0, 0), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(0), Array(0, 1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(0), Array(1, 1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(0), Array(-1, -1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(1), Array(0, 0), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(1), Array(0, 1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(1), Array(1, 1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(1), Array(-1, -1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(-1), Array(0, 0), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(-1), Array(0, 1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(-1), Array(1, 1), true, true, true, "non_carrier_proband"),
      ("X", "Female", false, Array(0, 0), Array(-1), Array(-1, -1), true, true, true, "non_carrier_proband"),

      // Les combinaisons avec gender=Male , calls=1 (affected_status=true)
      ("X", "Male", false, Array(1), Array(0), Array(0, 0), true, false, false, "x_linked_recessive_de_novo"),
      ("X", "Male", false, Array(1), Array(0), Array(0, 0), true, false, true, null),
      ("X", "Male", false, Array(1), Array(0), Array(0, 0), true, true, false, null),
      ("X", "Male", false, Array(1), Array(0), Array(0, 0), true, true, true, null),
      ("X", "Male", false, Array(1), Array(0), Array(0, 1), true, false, false, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(0), Array(0, 1), true, false, true, null),
      ("X", "Male", false, Array(1), Array(0), Array(0, 1), true, true, false, null),
      ("X", "Male", false, Array(1), Array(0), Array(0, 1), true, true, true, null),
      ("X", "Male", false, Array(1), Array(0), Array(1, 1), true, false, false, null),
      ("X", "Male", false, Array(1), Array(0), Array(1, 1), true, false, true, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(0), Array(1, 1), true, true, false, null),
      ("X", "Male", false, Array(1), Array(0), Array(1, 1), true, true, true, null),
      ("X", "Male", false, Array(1), Array(0), Array(-1, -1), true, false, false, null),
      ("X", "Male", false, Array(1), Array(0), Array(-1, -1), true, false, true, null),
      ("X", "Male", false, Array(1), Array(0), Array(-1, -1), true, true, false, null),
      ("X", "Male", false, Array(1), Array(0), Array(-1, -1), true, true, true, null),
      ("X", "Male", false, Array(1), Array(1), Array(0, 0), true, false, false, null),
      ("X", "Male", false, Array(1), Array(1), Array(0, 0), true, false, true, null),
      ("X", "Male", false, Array(1), Array(1), Array(0, 0), true, true, false, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(1), Array(0, 0), true, true, true, null),
      ("X", "Male", false, Array(1), Array(1), Array(0, 1), true, false, false, null),
      ("X", "Male", false, Array(1), Array(1), Array(0, 1), true, false, true, null),
      ("X", "Male", false, Array(1), Array(1), Array(0, 1), true, true, false, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(1), Array(0, 1), true, true, true, null),
      ("X", "Male", false, Array(1), Array(1), Array(1, 1), true, false, false, null),
      ("X", "Male", false, Array(1), Array(1), Array(1, 1), true, false, true, null),
      ("X", "Male", false, Array(1), Array(1), Array(1, 1), true, true, false, null),
      ("X", "Male", false, Array(1), Array(1), Array(1, 1), true, true, true, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(1), Array(-1, -1), true, false, false, null),
      ("X", "Male", false, Array(1), Array(1), Array(-1, -1), true, false, true, null),
      ("X", "Male", false, Array(1), Array(1), Array(-1, -1), true, true, false, null),
      ("X", "Male", false, Array(1), Array(1), Array(-1, -1), true, true, true, null),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 0), true, false, false, null),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 0), true, false, true, null),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 0), true, true, false, null),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 0), true, true, true, null),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 1), true, false, false, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 1), true, false, true, null),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 1), true, true, false, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(-1), Array(0, 1), true, true, true, null),
      ("X", "Male", false, Array(1), Array(-1), Array(1, 1), true, false, false, null),
      ("X", "Male", false, Array(1), Array(-1), Array(1, 1), true, false, true, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(-1), Array(1, 1), true, true, false, null),
      ("X", "Male", false, Array(1), Array(-1), Array(1, 1), true, true, true, "x_linked_recessive"),
      ("X", "Male", false, Array(1), Array(-1), Array(-1, -1), true, false, false, null),
      ("X", "Male", false, Array(1), Array(-1), Array(-1, -1), true, false, true, null),
      ("X", "Male", false, Array(1), Array(-1), Array(-1, -1), true, true, false, null),
      ("X", "Male", false, Array(1), Array(-1), Array(-1, -1), true, true, true, null),

      // Les combinaisons avec gender=Female , calls=1/1 (affected_status=true)
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 0), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 0), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 0), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 0), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 1), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(0, 1), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(1, 1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(1, 1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(1, 1), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(1, 1), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(-1, -1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(-1, -1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(-1, -1), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(0), Array(-1, -1), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 0), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 0), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 0), true, true, false, "x_linked_recessive"),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 0), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 1), true, true, false, "x_linked_recessive"),
      ("X", "Female", false, Array(1, 1), Array(1), Array(0, 1), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(1, 1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(1, 1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(1, 1), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(1, 1), true, true, true, "x_linked_recessive"),
      ("X", "Female", false, Array(1, 1), Array(1), Array(-1, -1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(-1, -1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(1), Array(-1, -1), true, true, false, "x_linked_dominant"),
      ("X", "Female", false, Array(1, 1), Array(1), Array(-1, -1), true, true, true, "x_linked_dominant"),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 0), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 0), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 0), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 0), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 1), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(0, 1), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(1, 1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(1, 1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(1, 1), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(1, 1), true, true, true, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(-1, -1), true, false, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(-1, -1), true, false, true, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(-1, -1), true, true, false, null),
      ("X", "Female", false, Array(1, 1), Array(-1), Array(-1, -1), true, true, true, null),

      // Les combinaisons avec gender=Female , calls=0/1 (affected_status=true)
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 0), true, false, false, "x_linked_dominant_de_novo"),
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 0), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 0), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 0), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 1), true, false, true, "x_linked_dominant"),
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(0, 1), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(1, 1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(1, 1), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(1, 1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(1, 1), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(-1, -1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(-1, -1), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(-1, -1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(0), Array(-1, -1), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 0), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 0), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 0), true, true, false, "x_linked_dominant"),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 0), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 1), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(0, 1), true, true, true, "x_linked_dominant"),
      ("X", "Female", false, Array(0, 1), Array(1), Array(1, 1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(1, 1), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(1, 1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(1, 1), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(-1, -1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(-1, -1), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(-1, -1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(1), Array(-1, -1), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 0), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 0), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 0), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 0), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 1), true, false, true, "x_linked_dominant"),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(0, 1), true, true, true, "x_linked_dominant"),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(1, 1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(1, 1), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(1, 1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(1, 1), true, true, true, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(-1, -1), true, false, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(-1, -1), true, false, true, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(-1, -1), true, true, false, null),
      ("X", "Female", false, Array(0, 1), Array(-1), Array(-1, -1), true, true, true, null),
    )
      .toDF("chromosome", "gender", "is_multi_allelic", "proband_calls", "father_calls", "mother_calls",
        "affected_status", "father_affected_status", "mother_affected_status", "expected_transmission")

    val result = input_occurrences
      .withGenotypeTransmission("transmission",
        "proband_calls",
        "gender",
        "affected_status",
        "father_calls",
        "father_affected_status",
        "mother_calls",
        "mother_affected_status")
      .where(
        functions.not(col("expected_transmission") === col("transmission")) or
          (col("expected_transmission").isNotNull and col("transmission").isNull) or
          (col("expected_transmission").isNull and col("transmission").isNotNull))

    result.show(false)
    result
      .select("expected_transmission", "transmission")
      .as[(String, String)]
      .collect() shouldBe Array.empty[(String, String)] //makes it easy to debug in case the test fails

  }

  it should "compute transmissions per locus" in {

    val df = Seq(
      ("1-222-A-G", "autosomal_dominant"),
      ("1-222-A-G", "autosomal_dominant"),
      ("2-222-A-G", "autosomal_recessive"),
      ("2-222-A-G", "denovo"),
      ("X-222-A-G", "x_linked_recessive")
    ).toDF("locus", "transmission")

    val resultDf = df.withTransmissionPerLocus(Seq("locus"), "transmission", "result")
    resultDf.show(false)

    resultDf.select("result").as[Map[String, Long]].collect() should contain allElementsOf Seq(
      Map("autosomal_dominant" -> 2),
      Map("autosomal_recessive" -> 1, "denovo" -> 1),
      Map("x_linked_recessive" -> 1)
    )

  }

  "sortChromosome" should "return chromosome as integers" in {
    val occurrences: DataFrame = Seq(
      "1",
      "10",
      "X",
      "Y",
      "M"
    ).toDF("chromosome")
    val frame: DataFrame = occurrences.select(sortChromosome)
    val res: Array[Int] = frame.as[Int].collect()
    res should contain theSameElementsAs Seq(1, 10, 100, 101, 102)
  }

}

