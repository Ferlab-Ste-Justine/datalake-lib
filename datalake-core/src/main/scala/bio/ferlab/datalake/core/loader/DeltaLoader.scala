package bio.ferlab.datalake.core.loader

import bio.ferlab.datalake.core.etl.Partitioning
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object DeltaLoader extends Loader {

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      uidName: String,
                      partitioning: Partitioning)(implicit spark: SparkSession): DataFrame = {

    require(updates.columns.exists(_.equals(uidName)), s"requires column [$uidName]")

    Try(DeltaTable.forName(s"$databaseName.$tableName")) match {
      case Failure(_) => writeOnce(location, databaseName, tableName, updates, partitioning)
      case Success(existing) =>

        /** Merge */
        val existingDf = existing.toDF
        existing.as("existing")
          .merge(
            updates.as("updates"),
            updates(uidName) === existingDf(uidName))
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
    }

    DeltaTable.forName(s"$databaseName.$tableName").toDF
  }

  def scd1(location: String,
           databaseName: String,
           tableName: String,
           updates: DataFrame,
           uidName: String,
           oidName: String,
           createdOnName: String,
           updatedOnName: String,
           partitioning: Partitioning)(implicit spark: SparkSession): DataFrame = {

    require(updates.columns.exists(_.equals(uidName)), s"requires column [$uidName]")
    require(updates.columns.exists(_.equals(oidName)), s"requires column [$oidName]")
    require(updates.columns.exists(_.equals(createdOnName)), s"requires column [$createdOnName]")
    require(updates.columns.exists(_.equals(updatedOnName)), s"requires column [$updatedOnName]")

    Try(DeltaTable.forName(s"$databaseName.$tableName")) match {
      case Failure(_) => writeOnce(location, databaseName, tableName, spark.table(tableName), partitioning)
      case Success(existing) =>
        val existingDf = existing.toDF
        existing.as("existing")
          .merge(
            updates.as("updates"),
            updates(uidName) === existingDf(uidName) && updates(oidName) =!= existingDf(oidName)
          )
          .whenMatched()
          .updateExpr(updates.columns.filterNot(_.equals(createdOnName)).map(c => c -> s"updates.$c").toMap)
          .whenNotMatched()
          .insertAll()
          .execute()
    }
    DeltaTable.forName(s"$databaseName.$tableName").toDF
  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: Partitioning,
                         dataChange: Boolean)(implicit spark: SparkSession): DataFrame = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    df
      .repartition(1, partitioning.repartitionExpr: _*)
      .sortWithinPartitions(partitioning.sortWithinPartitions:_*)
      .write
      .option("dataChange", dataChange)
      .mode(SaveMode.Overwrite)
      .partitionBy(partitioning.partitionBy: _*)
      .format("delta")
      .option("path", s"$location")
      .saveAsTable(s"$databaseName.$tableName")
    df
  }

  /**
   * Default read logic for a loader
   *
   * @param location    absolute path of where the data is
   * @param format      string representing the format
   * @param readOptions read options
   * @param spark       spark session
   * @return the data as a dataframe
   */
  override def read(location: String, format: String, readOptions: Map[String, String])
                   (implicit spark: SparkSession): DataFrame = {
    spark.read.format(format).load(location)
  }
}
