package bio.ferlab.datalake.spark3.publictables.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.time.LocalDateTime

case class SpliceAi(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_spliceai")
  val spliceai_indel: DatasetConf = conf.getDataset("normalized_spliceai_indel")
  val spliceai_snv: DatasetConf = conf.getDataset("normalized_spliceai_snv")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] = {
    Map(
      spliceai_indel.id -> spliceai_indel.read,
      spliceai_snv.id -> spliceai_snv.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val spliceai_snvDf = data(spliceai_snv.id)
    val spliceai_indelDf = data(spliceai_indel.id)

    val originalColumns = spliceai_snvDf.columns.map(col)

    val getDs: Column => Column = _.getItem(0).getField("ds") // Get delta score
    val scoreColumnNames = Array("AG", "AL", "DG", "DL")
    val scoreColumns = scoreColumnNames.map(c => array(struct(col(c) as "ds", lit(c) as "type")))
    val maxScore: Column = scoreColumns.reduce {
      (c1, c2) =>
        when(getDs(c1) > getDs(c2), c1)
          .when(getDs(c1) === getDs(c2), concat(c1, c2))
          .otherwise(c2)
    }

    spliceai_snvDf
      .union(spliceai_indelDf)
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
      .select(originalColumns :+ $"max_score": _*)
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(500))

}

object SpliceAi {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    SpliceAi(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
