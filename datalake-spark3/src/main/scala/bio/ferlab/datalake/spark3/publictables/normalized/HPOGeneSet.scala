package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf}
import bio.ferlab.datalake.spark3.etl.RuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

case class HPOGeneSet(rc: RuntimeETLContext) extends SimpleETLP(rc) {
  private val hpo_gene_set = conf.getDataset("raw_hpo_gene_set")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_hpo_gene_set")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      hpo_gene_set.id -> hpo_gene_set.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(hpo_gene_set.id)
      .withColumnRenamed("_c0", "entrez_gene_id")
      .withColumnRenamed("_c1", "symbol")
      .withColumnRenamed("_c2", "hpo_term_id")
      .withColumnRenamed("_c3", "hpo_term_name")
      .withColumnRenamed("_c4", "frequency_raw")
      .withColumnRenamed("_c5", "frequency_hpo")
      .withColumnRenamed("_c6", "source_info")
      .withColumnRenamed("_c7", "source")
      .withColumnRenamed("_c8", "source_id")


  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}

object HPOGeneSet {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    HPOGeneSet(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
