package bio.ferlab.datalake.spark3.publictables.normalized.cosmic

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDateTime
import scala.collection.mutable

case class CosmicGeneSet(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  private val cosmic_gene_set = conf.getDataset("raw_cosmic_gene_set")
  override val mainDestination: DatasetConf = conf.getDataset("normalized_cosmic_gene_set")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
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
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    spark.udf.register("trim_array", trim_array_udf)

    val df = data(cosmic_gene_set.id)
      .select(
        regexp_extract($"Genome Location", "(.+):(\\d+)-(\\d+)", 1) as "chromosome",
        regexp_extract($"Genome Location", "(.+):(\\d+)-(\\d+)", 2).cast(LongType) as "start",
        $"Gene Symbol" as "symbol",
        $"Name" as "name",
        $"COSMIC ID" as "cosmic_gene_id",
        $"Tier".cast(IntegerType).as("tier"),
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

object CosmicGeneSet {
  @main
  def run(rc: RuntimeETLContext): Unit = {
    CosmicGeneSet(rc).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
