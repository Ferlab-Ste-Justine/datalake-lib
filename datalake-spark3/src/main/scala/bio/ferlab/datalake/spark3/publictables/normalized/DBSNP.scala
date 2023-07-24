package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class DBSNP(rc: RuntimeETLContext) extends SimpleETLP(rc)  {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_dbsnp")

  private val raw_dbsnp = conf.getDataset("raw_dbsnp")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(raw_dbsnp.id -> raw_dbsnp.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    data(raw_dbsnp.id)
      .where($"contigName" like "NC_%")
      .withColumn("chromosome", regexp_extract($"contigName", "NC_(\\d+).(\\d+)", 1).cast("int"))
      .select(
        when($"chromosome" === 23, "X")
          .when($"chromosome" === 24, "Y")
          .when($"chromosome" === 12920, "M")
          .otherwise($"chromosome".cast("string")) as "chromosome",
        start,
        end,
        name,
        reference,
        alternate,
        $"contigName" as "original_contig_name"
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq("start"))

}

object DBSNP {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    DBSNP(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
