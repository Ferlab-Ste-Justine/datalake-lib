package bio.ferlab.datalake.spark3.genomics.prepared

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

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
 * @param configuration the configuration object
 */
class VariantCentric(implicit configuration: Configuration) extends ETLSingleDestination {
  override val mainDestination: DatasetConf = conf.getDataset("es_index_variant_centric")
  private val enriched_variants: DatasetConf = conf.getDataset("enriched_variants")
  private val enriched_consequences: DatasetConf = conf.getDataset("enriched_consequences")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {

    Map(
      enriched_variants.id -> enriched_variants.read,
      enriched_consequences.id -> enriched_consequences.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
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
      .agg(min("consequences.sort_csq") as "sort_csq", array_sort(collect_list("consequences") as "consequences"))
      //cleanup sort_csq
      .withColumn("consequences", functions.transform(col("consequences"), c => c.dropFields("sort_csq")) )
      .withColumn("consequences", struct(
        //order array of gene by sort_csq
        col("sort_csq").as("sort_gene"),
        col("symbol"), col("consequences")))
      .groupByLocus()
      .agg(array_sort(collect_list("consequences")) as "consequences")
      .withColumn("consequences", functions.transform(col("consequences"), c => c.dropFields("sort_gene")) )
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
      .withColumn("csq", flatten(col("genes.consequences")))
      .withColumn("max_impact_score", array_max(col("csq.impact_score")))
      .drop("csq")

    joinedVariants
  }
}
