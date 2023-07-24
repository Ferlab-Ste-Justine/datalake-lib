package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class DDDGeneSet(rc: RuntimeETLContext) extends SimpleETLP(rc) {
  private val ddd_gene_set = conf.getDataset("raw_ddd_gene_set")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_ddd_gene_set")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      ddd_gene_set.id -> ddd_gene_set.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    data(ddd_gene_set.id)
      .select(
        $"gene symbol" as "symbol",
        $"gene mim" as "omim_gene_id",
        $"disease name" as "disease_name",
        $"disease mim" as "disease_omim_id",
        $"confidence category" as "confidence_category",
        $"mutation consequence" as "mutation_consequence",
        split($"variant consequence", ";") as "variant_consequence",
        split($"phenotypes", ";") as "phenotypes",
        split($"organ specificity list", ";") as "organ_specificity",
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