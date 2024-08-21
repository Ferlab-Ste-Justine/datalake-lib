package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import mainargs.{ParserForMethods, arg, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.time.LocalDateTime

case class SpliceAi(rc: RuntimeETLContext, variantType: String) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset(s"enriched_spliceai_$variantType")
  val normalized_spliceai: DatasetConf = conf.getDataset(s"normalized_spliceai_$variantType")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(normalized_spliceai.id -> normalized_spliceai.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val df = data(normalized_spliceai.id)
    val originalColumns = df.columns.map(col)

    val getDs: Column => Column = _.getItem(0).getField("ds") // Get delta score
    val scoreColumnNames = Array("AG", "AL", "DG", "DL")
    val scoreColumns = scoreColumnNames.map(c => array(struct(col(c) as "ds", lit(c) as "type")))
    val maxScore: Column = scoreColumns.reduce {
      (c1, c2) =>
        when(getDs(c1) > getDs(c2), c1)
          .when(getDs(c1) === getDs(c2), concat(c1, c2))
          .otherwise(c2)
    }

    df
      .select(
        originalColumns :+
          $"ds_ag".as("AG") :+ // acceptor gain
          $"ds_al".as("AL") :+ // acceptor loss
          $"ds_dg".as("DG") :+ // donor gain
          $"ds_dl".as("DL"): _* // donor loss
      )
      .withColumn("max_score_temp", maxScore)
      .withColumn("max_score", struct(
        getDs($"max_score_temp") as "ds",
        functions.transform($"max_score_temp", c => c.getField("type")) as "type")
      )
      .withColumn("max_score", $"max_score".withField("type", when($"max_score.ds" === 0, null).otherwise($"max_score.type")))
      .select(originalColumns :+ $"max_score": _*)
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(500))
}

object SpliceAi {
  @main
  def run(rc: RuntimeETLContext, @arg(name = "variant_type", short = 'v', doc = "Variant Type") variantType: String): Unit = {
    SpliceAi(rc, variantType).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
