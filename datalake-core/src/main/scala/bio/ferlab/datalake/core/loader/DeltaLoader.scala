package bio.ferlab.datalake.core.loader

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object DeltaLoader extends Loader {

  override def upsert(location: String,
                      tableName: String,
                      updates: DataFrame,
                      uidName: String,
                      repartitionExpr: Seq[Column] = Seq(),
                      sortWithinPartitions: Seq[Column] = Seq(),
                      partitionBy: Seq[String] = Seq(),
                      dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame = {

      require(updates.columns.exists(_.equals(uidName)), s"requires column [$uidName]")

      Try(DeltaTable.forName(tableName)) match {
        case Failure(_) => writeOnce(location, tableName, updates)
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

      DeltaTable.forName(tableName).toDF
    }

  def upsertAndCompact(location: String,
                       tableName: String,
                       updates: DataFrame,
                       uidName: String)(implicit spark: SparkSession): DataFrame = {

    /** Upsert */
    upsert(location, tableName, updates, uidName)

    /** Compact */
    writeOnce(location, tableName, spark.table(tableName), dataChange = false)
  }

    def scd1(location: String,
             tableName: String,
             updates: DataFrame,
             uidName: String,
             oidName: String,
             createdOnName: String,
             updatedOnName: String,
             repartitionExpr: Seq[Column] = Seq(),
             sortWithinPartitions: Seq[Column] = Seq(),
             partitionBy: Seq[String] = Seq(),
             dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame = {

      require(updates.columns.exists(_.equals(uidName)), s"requires column [$uidName]")
      require(updates.columns.exists(_.equals(oidName)), s"requires column [$oidName]")
      require(updates.columns.exists(_.equals(createdOnName)), s"requires column [$createdOnName]")
      require(updates.columns.exists(_.equals(updatedOnName)), s"requires column [$updatedOnName]")

      Try(DeltaTable.forName(tableName)) match {
        case Failure(_) => writeOnce(location, tableName, spark.table(tableName))
        case Success(existing) =>

          /** Merge */
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

          /** Compact */
          writeOnce(location, tableName, spark.table(tableName), dataChange = false)
      }
    }

  def scd1AndCompact(location: String,
                     tableName: String,
                     updates: DataFrame,
                     uidName: String,
                     oidName: String,
                     createdOnName: String,
                     updatedOnName: String)(implicit spark: SparkSession): DataFrame = {
    /** scd1 */
    scd1(location, tableName, updates, uidName, oidName, createdOnName, updatedOnName)
    /** Compact */
    writeOnce(location, tableName, spark.table(tableName), dataChange = false)
  }

    override def writeOnce(location: String,
                           tableName: String,
                           df: DataFrame,
                           repartitionExpr: Seq[Column] = Seq(),
                           sortWithinPartitions: Seq[Column] = Seq(),
                           partitionBy: Seq[String] = Seq(),
                           dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame = {
      df
        .repartition(1, repartitionExpr: _*)
        .sortWithinPartitions(sortWithinPartitions:_*)
        .write
        .option("dataChange", dataChange)
        .mode(SaveMode.Overwrite)
        .partitionBy(partitionBy: _*)
        .format("delta")
        .option("path", s"$location")
        .saveAsTable(s"$tableName")
      df
    }

}
