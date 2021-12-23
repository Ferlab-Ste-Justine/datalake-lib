package bio.ferlab.datalake.spark3.loader

import io.projectglow.Glow
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object VcfLoader extends Loader {

  override def read(location: String,
                    format: String,
                    readOptions: Map[String, String],
                    databaseName: Option[String],
                    tableName: Option[String])(implicit spark: SparkSession): DataFrame = {

    val df = spark.read.format(format)
      .option("flattenInfoFields", readOptions.getOrElse("flattenInfoFields", "true"))
      .load(location.split(","): _*)
      .withColumnRenamed("filters", "INFO_FILTERS") // Avoid losing filters columns before split

    val splitMultiAllelicDf: DataFrame =
      readOptions
        .get("split_multiallelics")
        .fold(df){_ => Glow.transform("split_multiallelics", df)}

    val normalizedDf =
      (readOptions.get("normalize_variants"), readOptions.get("reference_genome_path")) match {
        case (Some("true"), Some(path)) => Glow.transform("normalize_variants", df, ("reference_genome_path", path))
        case _ => splitMultiAllelicDf
          .withColumn("normalizationStatus",
            struct(
              lit(false) as "changed",
              lit(null).cast(StringType) as "errorMessage"))
      }
    normalizedDf
  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName:  String,
                         df:  DataFrame,
                         partitioning: List[String],
                         format: String,
                         options: Map[String, String])
                        (implicit spark:  SparkSession): DataFrame = ???

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates:  DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])
                     (implicit spark:  SparkSession): DataFrame = ???

  override def scd1(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: List[String],
                    format: String)
                   (implicit spark:  SparkSession): DataFrame = ???

  override def insert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

  override def scd2(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: List[String],
                    format: String,
                    validFromName: String,
                    validToName: String,
                    minValidFromDate: LocalDate,
                    maxValidToDate: LocalDate)(implicit spark: SparkSession): DataFrame = ???

  override def overwritePartition(location: String,
                                  databaseName: String,
                                  tableName: String,
                                  df: DataFrame,
                                  partitioning: List[String],
                                  format: String,
                                  options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???
}
