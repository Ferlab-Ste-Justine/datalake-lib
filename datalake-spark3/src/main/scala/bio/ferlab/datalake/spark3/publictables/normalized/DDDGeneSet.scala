package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class DDDGeneSet(rc: RuntimeETLContext) extends SimpleETLP(rc) {
  private val ddd_gene_set = conf.getDataset("raw_ddd_gene_set")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_ddd_gene_set")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      ddd_gene_set.id -> ddd_gene_set.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    data(ddd_gene_set.id)
      .select(
        $"gene symbol" as "symbol",
        $"gene mim" as "omim_gene_id",
        $"disease name" as "disease_name",
        $"disease mim" as "disease_omim_id",
        $"confidence" as "confidence_category",
        $"variant consequence" as "mutation_consequence",
        split($"variant types", "; ") as "variant_consequence",
        split($"phenotypes", "; ") as "phenotypes",
        $"panel",
        $"hgnc id" as "hgnc_id"
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}

object DDDGeneSet {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    DDDGeneSet(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}