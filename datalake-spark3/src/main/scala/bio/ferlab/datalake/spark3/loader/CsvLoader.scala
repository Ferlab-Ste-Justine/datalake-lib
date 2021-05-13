package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.spark3.etl.Partitioning
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvLoader extends Loader {

  override def read(location: String, format: String, readOptions: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    spark.read.options(readOptions).csv(location)
  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName:  String,
                         df:  DataFrame,
                         partitioning: Partitioning,
                         dataChange: Boolean)
                        (implicit spark:  SparkSession): DataFrame = {
    val repartitionedData = partitioning.repartitionExpr(df).persist()
    repartitionedData
      .write
      .mode("overwrite")
      .partitionBy(partitioning.partitionBy:_*)
      .save(location)
    repartitionedData
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
