package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.SparkUtils.{array_remove_empty, getColumnOrElse}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class GenesSuggestions(rc: RuntimeETLContext) extends SimpleSingleETL(rc)  {

  override val mainDestination: DatasetConf = conf.getDataset("es_index_gene_suggestions")

  final val geneSymbolWeight            = 5
  final val geneAliasesWeight           = 3

  final val indexColumns =
    List("type", "symbol", "suggestion_id", "suggest", "ensembl_gene_id")

  private val enriched_genes: DatasetConf = conf.getDataset("enriched_genes")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      enriched_genes.id -> enriched_genes.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val genes = data(enriched_genes.id).select("symbol", "alias", "ensembl_gene_id")

    getGenesSuggest(genes)
  }


  def getGenesSuggest(genes: DataFrame): DataFrame = {
    genes
      .withColumn("ensembl_gene_id", getColumnOrElse("ensembl_gene_id"))
      .withColumn("symbol", getColumnOrElse("symbol"))
      .withColumn("type", lit("gene"))
      .withColumn("suggestion_id", sha1(col("symbol"))) //this maps to `hash` column in gene_centric index
      .withColumn(
        "suggest",
        array(
          struct(
            array(col("symbol")) as "input",
            lit(geneSymbolWeight) as "weight"
          ),
          struct(
            array_remove_empty(
              flatten(
                array(
                  functions.transform(col("alias"), c => when(c.isNull, lit("")).otherwise(c)),
                  array(col("ensembl_gene_id"))
                )
              )
            ) as "input",
            lit(geneAliasesWeight) as "weight"
          )
        )
      )
      .select(indexColumns.head, indexColumns.tail: _*)
  }
}

object GenesSuggestions {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    GenesSuggestions(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
