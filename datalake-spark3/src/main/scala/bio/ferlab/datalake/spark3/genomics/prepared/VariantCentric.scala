package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, functions}

import java.time.LocalDateTime

/**
 * This ETL is used to prepare data that will be indexed into ElasticSearch. It is used to create a variant centric index.
 * Each row contain a variant. The genes field is an array of genes, each gene contains an array of consequences.
 * Consequences that have no gene are grouped under a fake gene with symbol "NO_GENE".
 *
 * @example
 * For simplicity, columns not relevant to the example are omitted.
 * In variant table:
 * {{{
 * +----------+-----+---------+---------+-----------------------+
 * |chromosome|start|reference|alternate|symbols                |
 * +----------+-----+---------+---------+-----------------------+
 * |1         |69897|T        |C        |[GENE1, GENE2]         |
 * +----------+-----+---------+---------+-----------------------+
 * }}}
 *
 * In consequence table:
 * {{{
 * +----------+-----+---------+---------+--------+-------------+
 * |chromosome|start|reference|alternate|symbol  |transcript_id|
 * +----------+-----+---------+---------+--------+-------------+
 * |1         |69897|T        |C        |GENE1   |T1           |
 * |1         |69897|T        |C        |GENE1   |T2           |
 * |1         |69897|T        |C        |GENE2   |T3           |
 * |1         |69897|T        |C        |null    |T4           |
 * +----------+-----+---------+---------+-------=+-------------+
 * }}}
 * Will generate this row in the variant centric index:
 * {{{
 *   {
 *    "chromosome": "1",
 *    "start": 69897,
 *    "reference": "T",
 *    "alternate": "C",
 *    "locus": "1-69897-T-C",
 *    "genes": {
 *      "symbol": "GENE1",
 *      "consequences": [
 *        {
 *          "transcript_id": "T1"
 *          ....
 *        },
 *        {
 *          "transcript_id": "T2"
 *        },
 *        ....
 *      ],
 *      ....
 *      },
 *      {
 *      "symbol": "GENE2",
 *      "consequences": [
 *        {
 *          "transcript_id": "T3"
 *          ....
 *        }
 *        ....
 *      ],
 *      ....
 *      },
 *      {
 *      "symbol": "NO_GENE",
 *      "consequences": [
 *        {
 *          "transcript_id": "T4"
 *          ....
 *        }
 *        ....
 *      ],
 *      ....
 *      },
 *    }
 * }}}
 * @param rc the etl context
 * @param destinationDatasetId custom destination dataset id
 * @param enrichedVariantsDatasetId custom enrich_variants dataset id
 * @param enrichedConsequencesDatasetId custom enrich_consequences dataset id
 */
case class VariantCentric(rc: RuntimeETLContext,
                          destinationDatasetId: String = "es_index_variant_centric",
                          enrichedVariantsDatasetId: String = "enriched_variants",
                          enrichedConsequencesDatasetId: String = "enriched_consequences") extends SimpleSingleETL(rc) {
  override val mainDestination: DatasetConf = conf.getDataset(destinationDatasetId)
  private val enriched_variants: DatasetConf = conf.getDataset(enrichedVariantsDatasetId)
  private val enriched_consequences: DatasetConf = conf.getDataset(enrichedConsequencesDatasetId)

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(
      enriched_variants.id -> enriched_variants.read,
      enriched_consequences.id -> enriched_consequences.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val NO_GENE = "NO_GENE"
    val csq = data(enriched_consequences.id)
      .withColumn("symbol", coalesce(col("symbol"), lit(NO_GENE))) //Manage consequences without gene
      .drop("biotype", "ensembl_gene_id", "updated_on", "created_on", "consequences_oid", "normalized_consequences_oid", "original_canonical")
      .withColumn("picked", coalesce(col("picked"), lit(false)))

    val csqByGene = csq
      .withColumn("consequences", struct(
        //order array of xsq by picked
        when(col("picked") === true, 0).when(col("canonical") === true, 1).otherwise(2) as "sort_csq",
        csq("*")
      ).dropFields("symbol" :: columns.locusColumnNames: _*))
      .groupByLocus(col("symbol"))
      .agg(array_sort(collect_list("consequences")) as "consequences")
      //cleanup sort_csq
      //      .withColumn("consequences", functions.transform(col("consequences"), c => c.dropFields("sort_csq")))
      .withColumn("consequences", struct(
        col("symbol"), col("consequences")))
      .groupByLocus()
      .agg(collect_list("consequences") as "consequences")
      .withColumn("consequences", map_from_entries(col("consequences")))
      .selectLocus(col("consequences"))

    //Fake gene with empty values, used to join consequences without gene
    val geneStructType: StructType = data(enriched_variants.id).select("genes").schema.fields(0).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val noGeneStruct = struct(
      lit(NO_GENE).as("symbol") :: geneStructType.fields.toList.collect { case field if field.name != "symbol" => lit(null).cast(field.dataType).as(field.name) }: _*
    ).cast(geneStructType)

    val joinedVariants = data(enriched_variants.id)
      .drop("updated_on", "created_on")
      .withColumn("genes", array_union(col("genes"), array(noGeneStruct)))
      .joinByLocus(csqByGene, "left")
      .withColumn("genes", functions.transform(col("genes"), g => g.withField("consequences", col("consequences")(g("symbol")))))
      .drop("consequences")
      .withColumn("genes", functions.filter(col("genes"), g => not(g("symbol") === NO_GENE && g("consequences").isNull))) // cleanup no gene without consequences
      //sort gene by min sort_csq
      .withColumn("genes", functions.transform(col("genes"), g => {
        struct(array_min(g("consequences"))("sort_csq") as "sort_gene", g as "g")
      }))
      .withColumn("genes", array_sort(col("genes")))
      .withColumn("genes", functions.transform(col("genes"), g => g("g")))
      //Cleanup sort_csq into consequences
      .withColumn("genes", functions.transform(col("genes"), g => {
        val cleanupCsq = functions.transform(g("consequences"), c => c.dropFields("sort_csq"))
        g.dropFields("sort_gene", "consequences").withField("consequences", cleanupCsq)
      }))
      //calculate mac_impact_score
      .withColumn("csq", flatten(col("genes.consequences")))
      .withColumn("max_impact_score", array_max(col("csq.impact_score")))
      .drop("csq")

    joinedVariants
  }
}

object VariantCentric {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    VariantCentric(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
