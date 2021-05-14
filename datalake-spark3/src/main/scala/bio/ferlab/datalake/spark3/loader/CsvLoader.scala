package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvLoader extends Loader {

  override def read(location: String, format: String, readOptions: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    spark.read.options(readOptions).csv(location)
  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: List[String],
                         dataChange: Boolean)
                        (implicit spark:  SparkSession): DataFrame = {
    df
      .write
      .mode("overwrite")
      .partitionBy(partitioning:_*)
      .save(location)
    df
  }

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates:  DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String])
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
                    partitioning: List[String])
                   (implicit spark:  SparkSession): DataFrame = {
    throw NotImplementedException
  }
}
