package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.DELTA
import bio.ferlab.datalake.spark3.transformation.Implicits._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, concat_ws, lit, sha1}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.LocalDate
import scala.util.{Failure, Success, Try}

object DeltaLoader extends Loader {

  private def readTableAsDelta(location: String,
                               databaseName: String,
                               tableName: String): Try[DeltaTable] = {
    Try(DeltaTable.forName(s"$databaseName.$tableName"))
      .orElse(Try(DeltaTable.forPath(location)))
  }

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])(implicit spark: SparkSession): DataFrame = {

    require(primaryKeys.forall(updates.columns.contains), s"requires column [${primaryKeys.mkString(", ")}]")

    readTableAsDelta(location, databaseName, tableName) match {
      case Failure(_) =>
        writeOnce(location, databaseName, tableName, updates, partitioning, format)
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

    read(location, DELTA.sparkFormat, options, Some(databaseName), Some(tableName))
  }

  def insert(location: String,
             databaseName: String,
             tableName: String,
             updates: DataFrame,
             partitioning: List[String],
             format: String,
             options: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    GenericLoader.insert(location, databaseName, tableName, updates, partitioning, format, options)
    read(location, DELTA.sparkFormat, options, Some(databaseName), Some(tableName))
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

    readTableAsDelta(location, databaseName, tableName) match {
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
    read(location, DELTA.sparkFormat, Map(), Some(databaseName), Some(tableName))
  }

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: List[String],
                         format: String,
                         options: Map[String, String] = Map("dataChange" -> "true"))(implicit spark: SparkSession): DataFrame = {
    GenericLoader.writeOnce(location, databaseName, tableName, df, partitioning, format, options)
  }

  override def overwritePartition(location: String,
                                  databaseName: String,
                                  tableName: String,
                                  df: DataFrame,
                                  partitioning: List[String],
                                  format: String,
                                  options: Map[String, String] = Map("dataChange" -> "true"))(implicit spark: SparkSession): DataFrame = {
    if (partitioning.isEmpty)
      throw new IllegalArgumentException(s"Cannot use loadType 'OverWritePartition' without partitions.")

    val partitionValues = df.select(partitioning.head).distinct.collect().map(_.get(0))
    val replaceWhereClause = s"${partitioning.head} in ('${partitionValues.mkString("', '")}')"

    writeOnce(location, databaseName, tableName, df, partitioning, format, options + ("replaceWhere" -> replaceWhereClause))
  }

  override def read(location: String,
                    format: String,
                    readOptions: Map[String, String],
                    databaseName: Option[String],
                    tableName: Option[String])(implicit spark: SparkSession): DataFrame = {
    Try(GenericLoader.read(location, format, readOptions, databaseName, tableName))
      .getOrElse(DeltaTable.forPath(location).toDF)
  }

  def scd2(location: String,
           databaseName: String,
           tableName: String,
           updates: DataFrame,
           primaryKeys: Seq[String],
           buidName: String,
           oidName: String,
           isCurrentName: String,
           partitioning: List[String],
           format: String,
           validFromName: String,
           validToName: String,
           minValidFromDate: LocalDate = LocalDate.of(1900, 1, 1),
           maxValidToDate: LocalDate = LocalDate.of(9999, 12, 31))(implicit spark: SparkSession): DataFrame = {

    require(primaryKeys.forall(updates.columns.contains), s"requires column [${primaryKeys.mkString(", ")}]")
    require(updates.columns.exists(_.equals(oidName)), s"requires column [$oidName]")
    require(updates.columns.exists(_.equals(validFromName)), s"requires column [$validFromName]")

    val newData =
      updates
        .withColumn(buidName, sha1(concat_ws("_", primaryKeys.map(col):_*)))
        .withColumn(validToName, lit(maxValidToDate))
        .withColumn(isCurrentName, lit(true))

    val deduplicatedData =
      if (updates.select(validFromName).dropDuplicates().count() > 1)
        newData.dropDuplicates(Seq(buidName), col(validFromName).desc)
      else
        newData

    readTableAsDelta(location, databaseName, tableName) match {
      case Failure(_) => writeOnce(location, databaseName, tableName, deduplicatedData, partitioning, format)
      case Success(existing) =>
        val existingDf = existing.toDF

        val newRowsToInsert = deduplicatedData
          .as("updates")
          .join(existingDf, primaryKeys)
          .where(existingDf(isCurrentName) and deduplicatedData(oidName) =!= existingDf(oidName) and existingDf(validFromName) < deduplicatedData(validFromName))

        val stagedUpdates = newRowsToInsert
          .selectExpr("NULL as mergeKey", "updates.*")
          .union(
            deduplicatedData.selectExpr(s"${buidName} as mergeKey", "*")
          )

        val columnWithoutMergeKey = stagedUpdates.columns.filterNot(_.equals("mergeKey")).map(c => c -> s"updates.$c").toMap

        existing.as("existing")
          .merge(
            stagedUpdates.as("updates"),
            existingDf(buidName) === col("mergeKey")
          )
          .whenMatched(stagedUpdates(oidName) =!= existingDf(oidName) and existingDf(isCurrentName) and existingDf(validFromName) < stagedUpdates(validFromName))
          .updateExpr(Map(
            isCurrentName -> "false",
            validToName -> s"updates.$validFromName - 1 days"))
          .whenMatched(stagedUpdates(oidName) =!= existingDf(oidName) and existingDf(isCurrentName) and existingDf(validFromName) === stagedUpdates(validFromName))
          .updateExpr(columnWithoutMergeKey)
          .whenNotMatched()
          .insertExpr(columnWithoutMergeKey)
          .execute()
    }
    read(location, DELTA.sparkFormat, Map(), Some(databaseName), Some(tableName))
  }
}
