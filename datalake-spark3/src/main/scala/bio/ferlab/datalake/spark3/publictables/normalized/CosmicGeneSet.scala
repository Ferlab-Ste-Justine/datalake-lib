package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import org.apache.spark.sql.expressions.UserDefinedFunction
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.utils.Coalesce
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.collection.mutable

class CosmicGeneSet()(implicit conf: Configuration) extends ETLP {

  private val cosmic_gene_set = conf.getDataset("raw_cosmic_gene_set")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_cosmic_gene_set")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(cosmic_gene_set.id -> cosmic_gene_set.read)
  }

  def trim_array_udf: UserDefinedFunction = udf { array: mutable.WrappedArray[String] =>
    if (array != null) {
      array.map {
        case null => null
        case str => str.trim()
      }
    } else {
      array
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.udf.register("trim_array", trim_array_udf)

    val df = data(cosmic_gene_set.id)
      .withColumn("split_loc", split($"Genome Location", ":"))
      .withColumn("chromosome", $"split_loc"(0))
      .withColumn("start", split($"split_loc"(1), "-")(0).cast(LongType))
      .withColumn("end", split($"split_loc"(1), "-")(1).cast(LongType))
      .select(
        $"chromosome",
        $"start",
        $"end",
        $"Gene Symbol" as "symbol",
        $"Name" as "name",
        $"Entrez GeneId" as "entrez_gene_id",
        $"Tier".cast(IntegerType).as("tier"),
        $"Genome Location" as "genome_location",
        when($"Hallmark" === "Yes", true).otherwise(false) as "hallmark",
        $"Chr Band" as "chr_band",
        when($"Somatic" === "yes", true).otherwise(false) as "somatic",
        when($"Germline" === "yes", true).otherwise(false) as "germline",
        split($"Tumour Types(Somatic)", ",") as "tumour_types_somatic",
        split($"Tumour Types(Germline)", ",") as "tumour_types_germline",
        $"Cancer Syndrome" as "cancer_syndrome",
        split($"Tissue Type", ",") as "tissue_type",
        $"Molecular Genetics" as "molecular_genetics",
        split($"Role in Cancer", ",") as "role_in_cancer",
        split($"Mutation Types", ",") as "mutation_types",
        split($"Translocation Partner", ",") as "translocation_partner",
        when($"Other Germline Mut" === "yes", true).otherwise(false) as "other_germline_mutation",
        split($"Other Syndrome", ",") as "other_syndrome",
        split($"Synonyms", ",") as "synonyms"
      )

    df.schema.fields
      .collect { case s@StructField(_, ArrayType(StringType, _), _, _) =>
        s
      } // take only array type fields
      .foldLeft(df)((d, f) =>
        d.withColumn(f.name, trim_array_udf(col(f.name)))
      ) // apply trim on each elements of each array

  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
