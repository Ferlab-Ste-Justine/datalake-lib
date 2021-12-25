package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.DELTA
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

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

    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    df
      .write
      .options(options)
      .mode(SaveMode.Overwrite)
      .partitionBy(partitioning: _*)
      .format("delta")
      .option("path", s"$location")
      .saveAsTable(s"$databaseName.$tableName")
    df
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
