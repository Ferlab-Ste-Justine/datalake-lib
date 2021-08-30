package bio.ferlab.datalake.spark3.loader

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import java.time.LocalDate
import scala.util.{Failure, Success, Try}

object DeltaLoader extends Loader {

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String],
                      format: String)(implicit spark: SparkSession): DataFrame = {

    require(primaryKeys.forall(updates.columns.contains), s"requires column [${primaryKeys.mkString(", ")}]")

    Try(DeltaTable.forName(s"$databaseName.$tableName")) match {
      case Failure(_) => writeOnce(location, databaseName, tableName, updates, partitioning, format)
      case Success(existing) =>

        val existingDf = existing.toDF
        val mergeCondition: Column = primaryKeys.map(c => updates(c) === existingDf(c)).reduce((a, b) => a && b)

        /** Merge */
        existing.as("existing")
          .merge(
            updates.as("updates"),
            mergeCondition
          )
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
    }

    DeltaTable.forName(s"$databaseName.$tableName").toDF
  }

  def insert(location: String,
             databaseName: String,
             tableName: String,
             updates: DataFrame,
             partitioning: List[String],
             format: String)(implicit spark: SparkSession): DataFrame = {
    GenericLoader.insert(location, databaseName, tableName, updates, partitioning, "delta")
  }

  def scd1(location: String,
           databaseName: String,
           tableName: String,
           updates: DataFrame,
           primaryKeys: Seq[String],
           oidName: String,
           createdOnName: String,
           updatedOnName: String,
           partitioning: List[String],
           format: String)(implicit spark: SparkSession): DataFrame = {

    require(primaryKeys.forall(updates.columns.contains), s"requires column [${primaryKeys.mkString(", ")}]")
    require(updates.columns.exists(_.equals(oidName)), s"requires column [$oidName]")
    require(updates.columns.exists(_.equals(createdOnName)), s"requires column [$createdOnName]")
    require(updates.columns.exists(_.equals(updatedOnName)), s"requires column [$updatedOnName]")

    Try(DeltaTable.forName(s"$databaseName.$tableName")) match {
      case Failure(_) => writeOnce(location, databaseName, tableName, updates, partitioning, format)
      case Success(existing) =>
        val existingDf = existing.toDF
        val mergeCondition: Column =
          primaryKeys
            .map(c => updates(c) === existingDf(c))
            .reduce((a, b) => a && b) && updates(oidName) =!= existingDf(oidName)

        existing.as("existing")
          .merge(
            updates.as("updates"),
            mergeCondition
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
                         partitioning: List[String],
                         format: String,
                         dataChange: Boolean)(implicit spark: SparkSession): DataFrame = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    df
      .write
      .option("dataChange", dataChange)
      .mode(SaveMode.Overwrite)
      .partitionBy(partitioning: _*)
      .format("delta")
      .option("path", s"$location")
      .saveAsTable(s"$databaseName.$tableName")
    df
  }

  override def read(location: String, format: String, readOptions: Map[String, String])
                   (implicit spark: SparkSession): DataFrame = {
    spark.read.format(format).load(location)
  }

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table for which keeping the full historic is required.
   *
   * @param location      full path of where the data will be located
   * @param tableName     the name of the updated/created table
   * @param updates       new data to be merged with existing data
   * @param primaryKeys   name of the columns holding the unique id
   * @param oidName       name of the column holding the hash of the column that can change over time (or version number)
   * @param createdOnName name of the column holding the creation timestamp
   * @param updatedOnName name of the column holding the last update timestamp
   * @param spark         a valid spark session
   * @return the data as a dataframe
   */
  def scd2(location: String,
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
}
