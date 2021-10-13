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

  override def read(location: String,
                    format: String,
                    readOptions: Map[String, String],
                    databaseName: Option[String],
                    tableName: Option[String])(implicit spark: SparkSession): DataFrame = {
    GenericLoader.read(location, format, readOptions, databaseName, tableName)
  }

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
