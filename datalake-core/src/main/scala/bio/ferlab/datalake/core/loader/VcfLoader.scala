package bio.ferlab.datalake.core.loader

import bio.ferlab.datalake.core.etl.Partitioning
import io.projectglow.Glow
import org.apache.spark.sql.{DataFrame, SparkSession}

object VcfLoader extends Loader {

  override def read(location: String,
                    format: String = "vcf",
                    readOptions: Map[String, String] = Map())(implicit spark: SparkSession): DataFrame = {

    val inputs = location.split(",")
    val df = spark.read.format(format)
      .option("flattenInfoFields", "true")
      .load(inputs: _*)
      .withColumnRenamed("filters", "INFO_FILTERS") // Avoid losing filters columns before split

    Glow.transform("split_multiallelics", df)
  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName:  String,
                         df:  DataFrame,
                         partitioning: Partitioning,
                         dataChange: Boolean)
                        (implicit spark:  SparkSession): DataFrame = {
    throw NotImplementedException
  }

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates:  DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: Partitioning)
                     (implicit spark:  SparkSession): DataFrame = {
    throw NotImplementedException
  }

  override def scd1(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: Partitioning)
                   (implicit spark:  SparkSession): DataFrame = {
    throw NotImplementedException
  }
}
