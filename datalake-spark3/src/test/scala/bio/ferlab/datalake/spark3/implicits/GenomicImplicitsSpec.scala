package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, functions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.ParentalOrigin._
import bio.ferlab.datalake.spark3.testmodels.enriched.EnrichedGenes
import bio.ferlab.datalake.testutils.WithSparkSession
import bio.ferlab.datalake.testutils.models.genomicimplicits.{AlleleDepthOutput, CompoundHetInput, CompoundHetOutput, ConsequencesInput, FullCompoundHetOutput, Genotype, HCComplement, OtherCompoundHetInput, PickedConsequencesOuput, PossiblyCompoundHetOutput, PossiblyHCComplement, RefSeqAnnotation, RefSeqMrnaIdInput, RefSeqMrnaIdInputWithoutAnnotation, RefSeqMrnaIdInputWithoutRefSeq, RelativesGenotype, RelativesGenotypeOutput}
import org.slf4j

import scala.collection.Seq

class GenomicImplicitsSpec extends AnyFlatSpec with WithSparkSession with Matchers {

  import spark.implicits._

  implicit val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  spark.sparkContext.setLogLevel("WARN")

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

      //null should be transform to Array(-1,-1)
      ("1", true, Array(0, 1), Array(0, 1), Array(0, 0), "WT", null),
      ("1", true, Array(0, 1), Array(0, 1), null, "HET", POSSIBLE_FATHER),
      ("1", true, Array(0, 1), null, Array(0, 0), "HET", POSSIBLE_DENOVO),
      ("X", true, Array(0, 1), Array(1), Array(0, 0), "HET", FTH),
      ("Y", true, Array(1), Array(1), null, "HET", FTH),

      // Solo proband
      ("X", true, Array(0, 1), null, null, "HET", UNKNOWN),
      ("Y", true, Array(1), null, null, "HET", UNKNOWN),
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

  "getCompoundHet" should "return compound het for one patient and one gene" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), Some("father"))
    ).toDF()

    input.getCompoundHet("patient_id", "symbols").as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))))
    )
  }
  it should "return compound het for one patient and multiple genes" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1", "BRAF2"), Some("father")),
      CompoundHetInput("PA001", "1", 1050, "C", "G", Seq("BRAF1", "BRAF2")),
      CompoundHetInput("PA001", "1", 1070, "C", "G", Seq("BRAF2"), Some("father"))
    ).toDF()

    input.getCompoundHet("patient_id", "symbols").as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF2", Seq("1-1030-C-G", "1-1070-C-G")), HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T")), HCComplement("BRAF2", Seq("1-1000-A-T")))),
      CompoundHetOutput("PA001", "1", 1070, "C", "G", is_hc = true, Seq(HCComplement("BRAF2", Seq("1-1000-A-T"))))
    )

  }
  it should "return compound het for two patients and one gene" in {

    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), Some("father")),
      CompoundHetInput("PA001", "1", 1050, "C", "G", Seq("BRAF1")),
      CompoundHetInput("PA002", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      CompoundHetInput("PA002", "1", 1050, "C", "G", Seq("BRAF1"), Some("father")),
    ).toDF()

    input.getCompoundHet("patient_id", "symbols").as[CompoundHetOutput].collect() should contain theSameElementsAs Seq(
      CompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G")))),
      CompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T")))),
      CompoundHetOutput("PA002", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1050-C-G")))),
      CompoundHetOutput("PA002", "1", 1050, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))))
    )

  }

  "getPossiblyCompoundHet" should "return possibly compound het for many patients" in {
    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2")),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1", "BRAF2")),
      CompoundHetInput("PA001", "1", 1070, "C", "G", Seq("BRAF2")),
      CompoundHetInput("PA001", "1", 1090, "C", "G", Seq("BRAF3")),
      CompoundHetInput("PA002", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2")),
      CompoundHetInput("PA002", "1", 1030, "C", "G", Seq("BRAF1"))
    ).toDF()

    val result = input.getPossiblyCompoundHet("patient_id", "symbols").as[PossiblyCompoundHetOutput]
    result.collect() should contain theSameElementsAs Seq(
      PossiblyCompoundHetOutput("PA001", "1", 1000, "A", "T", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2), PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA001", "1", 1030, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2), PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA001", "1", 1070, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF2", 3))),
      PossiblyCompoundHetOutput("PA002", "1", 1000, "A", "T", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      PossiblyCompoundHetOutput("PA002", "1", 1030, "C", "G", is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
    )
  }

  "withCompoundHeterozygous" should "return enriched dataframe with compound het and possibly compound het" in {
    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), Some("father")),
      CompoundHetInput("PA001", "2", 2000, "C", "G", Seq("BRAF1"), Some("father"), "HOM")
    ).toDF()
    input.withCompoundHeterozygous().as[FullCompoundHetOutput].collect() should contain theSameElementsAs Seq(
      FullCompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G"))), is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      FullCompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))), is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      FullCompoundHetOutput("PA001", "2", 2000, "C", "G", is_hc = false, Nil, is_possibly_hc = false, Nil)
    )

  }

  it should "return enriched dataframe filtered with additional filters" in {
    val input = Seq(
      CompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      CompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), Some("father")),
      CompoundHetInput("PA001", "2", 2000, "C", "G", Seq("BRAF1"), Some("father"), "HOM"),
      CompoundHetInput("PA001", "3", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      CompoundHetInput("PA001", "3", 1030, "C", "G", Seq("BRAF1"), Some("father"))
    ).toDF()
    val filter: Column = col("chromosome") === "1"
    val result = input.withCompoundHeterozygous(additionalFilter = Some(filter))
    result.show(false)
    result.as[FullCompoundHetOutput].collect() should contain theSameElementsAs Seq(
      FullCompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G"))), is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      FullCompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))), is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      FullCompoundHetOutput("PA001", "2", 2000, "C", "G", is_hc = false, Nil, is_possibly_hc = false, Nil),
      FullCompoundHetOutput("PA001", "3", 1000, "A", "T", is_hc = false, Nil, is_possibly_hc = false, Nil),
      FullCompoundHetOutput("PA001", "3", 1030, "C", "G", is_hc = false, Nil, is_possibly_hc = false, Nil),
    )

  }

  it should "return enriched dataframe with other cols as input parameters" in {
    val input = Seq(
      OtherCompoundHetInput("PA001", "1", 1000, "A", "T", Seq("BRAF1", "BRAF2"), Some("mother")),
      OtherCompoundHetInput("PA001", "1", 1030, "C", "G", Seq("BRAF1"), Some("father")),
      OtherCompoundHetInput("PA001", "2", 2000, "C", "G", Seq("BRAF1"), Some("father"), "HOM")
    ).toDF()
    val result = input
      .withCompoundHeterozygous(patientIdColumnName = "other_patient_id", geneSymbolsColumnName = "other_symbols")
      .withColumnRenamed("other_patient_id", "patient_id")

    result.as[FullCompoundHetOutput].collect() should contain theSameElementsAs Seq(
      FullCompoundHetOutput("PA001", "1", 1000, "A", "T", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1030-C-G"))), is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      FullCompoundHetOutput("PA001", "1", 1030, "C", "G", is_hc = true, Seq(HCComplement("BRAF1", Seq("1-1000-A-T"))), is_possibly_hc = true, Seq(PossiblyHCComplement("BRAF1", 2))),
      FullCompoundHetOutput("PA001", "2", 2000, "C", "G", is_hc = false, Nil, is_possibly_hc = false, Nil)
    )

  }

  "pickRandomCsqPerLocus" should "return a single consequence for each locus" in {
    val testDf = Seq(
      ConsequencesInput(chromosome = "1", start = 1, reference = "A", alternate = "C", ensembl_transcript_id = "ENS1"),
      ConsequencesInput(chromosome = "2", start = 2, reference = "C", alternate = "A", ensembl_transcript_id = "ENS2"),
      ConsequencesInput(chromosome = "2", start = 2, reference = "C", alternate = "A", ensembl_transcript_id = "ENS3"),
      ConsequencesInput(chromosome = "2", start = 2, reference = "C", alternate = "T", ensembl_transcript_id = "ENS4"),
    ).toDF()

    val result = testDf.pickRandomCsqPerLocus()
    result.where($"chromosome" === "1").count() shouldBe 1
    result.where($"chromosome" === "2" and $"start" === 2 and $"reference" === "C" and $"alternate" === "A").count() shouldBe 1
    result.where($"chromosome" === "2" and $"start" === 2 and $"reference" === "C" and $"alternate" === "T").count() shouldBe 1
  }

  "withPickedCsqPerLocus" should "pick one consequence per locus according to prioritization algorithm" in {
    val genesDf = Seq(
      EnrichedGenes(symbol = "IN_OMIM", omim_gene_id = "1"),
      EnrichedGenes(symbol = "NOT_IN_OMIM", omim_gene_id = null),
    ).toDF()

    val csqDf = Seq(
      // Single csq is max_impact_score
      ConsequencesInput(chromosome = "1", ensembl_transcript_id = "ENST1", impact_score = 1),
      ConsequencesInput(chromosome = "1", ensembl_transcript_id = "ENST2", impact_score = 3), // picked

      // No csq in OMIM && no protein coding csq
      ConsequencesInput(chromosome = "2", ensembl_transcript_id = "ENST1", symbol = "NOT_IN_OMIM", impact_score = 1, biotype = "processed_transcript"), // picked at random
      ConsequencesInput(chromosome = "2", ensembl_transcript_id = "ENST2", symbol = "NOT_IN_OMIM", impact_score = 1, biotype = "processed_transcript"), // picked at random

      // No csq in OMIM && protein coding csq && mane select csq
      ConsequencesInput(chromosome = "3", ensembl_transcript_id = "ENST1", symbol = "NOT_IN_OMIM", impact_score = 2, biotype = "protein_coding", mane_select = true), // picked
      ConsequencesInput(chromosome = "3", ensembl_transcript_id = "ENST2", symbol = "NOT_IN_OMIM", impact_score = 2, biotype = "protein_coding", mane_select = false),

      // Csq in OMIM && mane select csq
      ConsequencesInput(chromosome = "4", ensembl_transcript_id = "ENST1", symbol = "IN_OMIM", impact_score = 3, mane_select = true), // picked
      ConsequencesInput(chromosome = "4", ensembl_transcript_id = "ENST2", symbol = "IN_OMIM", impact_score = 3, mane_select = false),

      // Csq in OMIM && no mane select csq && canonical csq
      ConsequencesInput(chromosome = "5", ensembl_transcript_id = "ENST1", symbol = "IN_OMIM", impact_score = 4, mane_select = false, canonical = true), // picked
      ConsequencesInput(chromosome = "5", ensembl_transcript_id = "ENST2", symbol = "IN_OMIM", impact_score = 4, mane_select = false, canonical = false),

      // Csq in OMIM && no mane select csq && no canonical csq && mane plus csq
      ConsequencesInput(chromosome = "6", ensembl_transcript_id = "ENST1", symbol = "IN_OMIM", impact_score = 5, mane_select = false, canonical = false, mane_plus = true), // picked
      ConsequencesInput(chromosome = "6", ensembl_transcript_id = "ENST2", symbol = "IN_OMIM", impact_score = 5, mane_select = false, canonical = false, mane_plus = false),

      // Csq in OMIM && no mane select csq && no canonical csq && no mane plus csq
      ConsequencesInput(chromosome = "7", ensembl_transcript_id = "ENST1", symbol = "IN_OMIM", impact_score = 6, mane_select = false, canonical = false, mane_plus = false), // picked at random
      ConsequencesInput(chromosome = "7", ensembl_transcript_id = "ENST2", symbol = "IN_OMIM", impact_score = 6, mane_select = false, canonical = false, mane_plus = false), // picked at random
    ).toDF()

    val result = csqDf.withPickedCsqPerLocus(genesDf)

    // Only one csq picked per variant
    result
      .where($"picked")
      .groupByLocus()
      .count()
      .select("count").as[String].collect() should contain only "1"

    // Single csq is max_impact_score
    result
      .where($"chromosome" === "1")
      .as[PickedConsequencesOuput].collect() should contain theSameElementsAs Seq(
      PickedConsequencesOuput(chromosome = "1", ensembl_transcript_id = "ENST1", impact_score = 1, picked = None),
      PickedConsequencesOuput(chromosome = "1", ensembl_transcript_id = "ENST2", impact_score = 3, picked = Some(true)), // picked
    )

    // No csq in OMIM && no protein coding csq (picked at random)
    result.where($"chromosome" === "2" && $"picked").count() shouldBe 1

    // No csq in OMIM && protein coding csq && mane select csq
    result
      .where($"chromosome" === "3")
      .as[PickedConsequencesOuput].collect() should contain theSameElementsAs Seq(
      PickedConsequencesOuput(chromosome = "3", ensembl_transcript_id = "ENST1", symbol = "NOT_IN_OMIM", impact_score = 2, biotype = "protein_coding", mane_select = true, picked = Some(true)), // picked
      PickedConsequencesOuput(chromosome = "3", ensembl_transcript_id = "ENST2", symbol = "NOT_IN_OMIM", impact_score = 2, biotype = "protein_coding", mane_select = false, picked = None),
    )

    // Csq in OMIM && mane select csq
    result
      .where($"chromosome" === "4")
      .as[PickedConsequencesOuput].collect() should contain theSameElementsAs Seq(
      PickedConsequencesOuput(chromosome = "4", ensembl_transcript_id = "ENST1", symbol = "IN_OMIM", impact_score = 3, mane_select = true, picked = Some(true)), // picked
      PickedConsequencesOuput(chromosome = "4", ensembl_transcript_id = "ENST2", symbol = "IN_OMIM", impact_score = 3, mane_select = false, picked = None),
    )

    // Csq in OMIM && no mane select csq && canonical csq
    result
      .where($"chromosome" === "5")
      .as[PickedConsequencesOuput].collect() should contain theSameElementsAs Seq(
      PickedConsequencesOuput(chromosome = "5", ensembl_transcript_id = "ENST1", symbol = "IN_OMIM", impact_score = 4, mane_select = false, canonical = true, picked = Some(true)), // picked
      PickedConsequencesOuput(chromosome = "5", ensembl_transcript_id = "ENST2", symbol = "IN_OMIM", impact_score = 4, mane_select = false, canonical = false, picked = None),
    )

    // Csq in OMIM && no mane select csq && no canonical csq && mane plus csq
    result
      .where($"chromosome" === "6")
      .as[PickedConsequencesOuput].collect() should contain theSameElementsAs Seq(
      PickedConsequencesOuput(chromosome = "6", ensembl_transcript_id = "ENST1", symbol = "IN_OMIM", impact_score = 5, mane_select = false, canonical = false, mane_plus = true, picked = Some(true)), // picked
      PickedConsequencesOuput(chromosome = "6", ensembl_transcript_id = "ENST2", symbol = "IN_OMIM", impact_score = 5, mane_select = false, canonical = false, mane_plus = false, picked = None),
    )

    // Csq in OMIM && no mane select csq && no canonical csq && no mane plus csq (picked at random)
    result.where($"chromosome" === "7" && $"picked").count() shouldBe 1
  }

  "withAlleleDepths" should "calculate ad fields adequately" in {
    val input = Seq(
      Seq(15, 5), Seq(10, 0), Seq(0, 0)
    )
    val ads = input.toDF("ad")

    val result = ads.withAlleleDepths().as[AlleleDepthOutput].collect()

    val expectedResult = Seq(
      AlleleDepthOutput(15, 5, 20, 0.25),
      AlleleDepthOutput(10, 0, 10, 0),
      AlleleDepthOutput(0, 0, 0, 0),
    )
    result should contain theSameElementsAs expectedResult

    val adsRenamed = input.toDF("adRenamed")
    val resultRenamed = adsRenamed.withAlleleDepths(col("adRenamed")).as[AlleleDepthOutput].collect()

    resultRenamed should contain theSameElementsAs expectedResult
  }

  "withRelativeGenotype" should "add genotype information columns of relatives" in {
    val input = Seq(
      RelativesGenotype(participant_id = "PT_1", family_id = Some("FA_1"), gq = 10, mother_id = Some("PT_2"), father_id = Some("PT_3")),
      RelativesGenotype(participant_id = "PT_2", family_id = Some("FA_1"), gq = 20, calls = Array(0, 1)),
      RelativesGenotype(participant_id = "PT_3", family_id = Some("FA_1"), gq = 30, calls = Array(1, 1), other = Some("popi")),
      RelativesGenotype(participant_id = "PT_4", gq = 40),
    ).toDF()

    val result = input.withRelativesGenotype(Seq("calls", "gq", "other"))
    result.as[RelativesGenotypeOutput].collect() should contain theSameElementsAs Seq(
      RelativesGenotypeOutput(participant_id = "PT_1", family_id = Some("FA_1"), gq = 10, mother_id = Some("PT_2"), father_id = Some("PT_3"), mother_calls = Some(Seq(0, 1)), mother_gq = Some(20), father_calls = Some(Seq(1, 1)), father_gq = Some(30), father_other = Some("popi")),
      RelativesGenotypeOutput(participant_id = "PT_2", family_id = Some("FA_1"), gq = 20, calls = Seq(0, 1)),
      RelativesGenotypeOutput(participant_id = "PT_3", family_id = Some("FA_1"), gq = 30, calls = Seq(1, 1), other = Some("popi")),
      RelativesGenotypeOutput(participant_id = "PT_4", gq = 40),
    )

  }

  "withRefseqMrnaId" should "populate refseq_mrna_id if column exists" in {
    val input = Seq(
      RefSeqMrnaIdInput(),
      RefSeqMrnaIdInput(id = "2", annotation = None),
      RefSeqMrnaIdInput(id = "3", annotation = Some(RefSeqAnnotation(RefSeq = None))),
      RefSeqMrnaIdInput(id = "4", annotation = Some(RefSeqAnnotation(RefSeq = Some("890"))))
    ).toDF()
    val result = input.withRefseqMrnaId().select("id", "refseq_mrna_id")
    result.as[(String, Seq[String])].collect() should contain theSameElementsAs Seq(
      ("1", Seq("123", "456")),
      ("2", null),
      ("3", null),
      ("4", Seq("890")),
    )
  }

  it should "populate refseq_mrna_id if annotation RefSeq field does not exist" in {
    val input = Seq(
      RefSeqMrnaIdInputWithoutRefSeq()
    ).toDF()
    input.withRefseqMrnaId().select("id", "refseq_mrna_id").as[(String, Seq[String])].collect() should contain theSameElementsAs Seq(
      ("1", null)
    )
  }
  it should "populate refseq_mrna_id if annotation field does not exist" in {
    val input = Seq(
      RefSeqMrnaIdInputWithoutAnnotation()
    ).toDF()
    input.withRefseqMrnaId().select("id", "refseq_mrna_id").as[(String, Seq[String])].collect() should contain theSameElementsAs Seq(
      ("1", null)
    )
  }

  it should "return an empty DataFrame if optional VCF is missing" in {
    val df = vcf(List("f1", "f2"), None, optional = true)
    df shouldEqual spark.emptyDataFrame
  }

  it should "throw an exception if VCF is missing when not optional" in {
    val exception = intercept[AnalysisException] {
      vcf(List("f1", "f2"), None, optional = false)
    }
    exception.getMessage should include("Path does not exist:")
  }

}
