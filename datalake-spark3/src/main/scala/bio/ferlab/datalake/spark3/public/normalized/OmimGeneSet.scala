package bio.ferlab.datalake.spark3.public.normalized

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.public.normalized.OmimPhenotype.parse_pheno
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class OmimGeneSet()(implicit conf: Configuration) extends ETLP {

  override val destination: DatasetConf = conf.getDataset("normalized_omim_gene_set")
  val raw_omim_genemap: DatasetConf = conf.getDataset("raw_omim_genemap")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(raw_omim_genemap.id -> raw_omim_genemap.read)
  }

  override def transform(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime,
                         currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    val intermediateDf =
      data(raw_omim_genemap.id)
        .select(
          col("_c0") as "chromosome",
          col("_c1") as "start",
          col("_c2") as "end",
          col("_c3") as "cypto_location",
          col("_c4") as "computed_cypto_location",
          col("_c5") as "omim_gene_id",
          split(col("_c6"), ", ") as "symbols",
          col("_c7") as "name",
          col("_c8") as "approved_symbol",
          col("_c9") as "entrez_gene_id",
          col("_c10") as "ensembl_gene_id",
          col("_c11") as "documentation",
          split(col("_c12"), ";") as "phenotypes"
        )

    val nullPhenotypes =
      intermediateDf
        .filter(col("phenotypes").isNull)
        .drop("phenotypes")
        .withColumn(
          "phenotype",
          lit(null).cast(
            "struct<name:string,omim_id:string,inheritance:array<string>,inheritance_code:array<string>>"
          )
        )

    intermediateDf
      .withColumn("raw_phenotype", explode(col("phenotypes")))
      .drop("phenotypes")
      .withColumn("phenotype", parse_pheno(col("raw_phenotype")))
      .drop("raw_phenotype")
      .unionByName(nullPhenotypes)
  }

  override def load(data: DataFrame,
                    lastRunDateTime: LocalDateTime,
                    currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame =
    super.load(data.coalesce(1), lastRunDateTime, currentRunDateTime)
}




