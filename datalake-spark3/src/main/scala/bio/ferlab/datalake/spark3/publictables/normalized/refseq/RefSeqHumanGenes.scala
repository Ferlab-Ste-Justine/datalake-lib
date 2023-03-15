package bio.ferlab.datalake.spark3.publictables.normalized.refseq

import bio.ferlab.datalake.commons.config.{Coalesce, Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class RefSeqHumanGenes()(implicit conf: Configuration) extends ETLP {
  private val raw_refseq_human_genes = conf.getDataset("raw_refseq_human_genes")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_human_genes")
  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(raw_refseq_human_genes.id -> raw_refseq_human_genes.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data(raw_refseq_human_genes.id)
      .select(
        $"#tax_id" as "tax_id",
        $"GeneID" as "entrez_gene_id",
        $"Symbol" as "symbol",
        $"LocusTag" as "locus_tag",
        split($"Synonyms", "\\|") as "synonyms",
        splitToMap($"dbXrefs") as "external_references",
        $"chromosome",
        $"map_location",
        $"description",
        $"type_of_gene",
        $"Symbol_from_nomenclature_authority" as "symbol_from_nomenclature_authority",
        $"Full_name_from_nomenclature_authority" as "full_name_from_nomenclature_authority",
        $"Nomenclature_status" as "nomenclature_status",
        split($"Other_designations", "\\|") as "other_designations",
        splitToMap($"Feature_type") as "feature_types"
      )
      .withColumn("ensembl_gene_id", $"external_references.ensembl")
      .withColumn("omim_gene_id", $"external_references.mim")

  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()

  val splitToMapFn: String => Option[Map[String, String]] = { line =>
    Option(line)
      .map { l =>
        val elements = l.split("\\|")
        val m = elements.map { e =>
          val Array(key, value) = e.split(":", 2)
          key.toLowerCase.replaceAll("/", "_").replaceAll("-", "_") -> value
        }
        m.toMap
      }
  }

  val splitToMap: UserDefinedFunction = udf(splitToMapFn)
}
