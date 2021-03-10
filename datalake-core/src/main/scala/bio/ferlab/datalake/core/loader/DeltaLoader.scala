package bio.ferlab.datalake.core.loader

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object DeltaLoader extends Loader {

  override def upsert(output: String,
                      tableName: String,
                      updates: DataFrame,
                      uidName: String)(implicit spark: SparkSession): DataFrame = {

      require(updates.columns.exists(_.equals(uidName)), s"requires column [$uidName]")

      Try(DeltaTable.forName(tableName)) match {
        case Failure(_) => writeOnce(updates, tableName, output)
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

  def upsertAndCompact(output: String,
                       tableName: String,
                       updates: DataFrame,
                       uidName: String)(implicit spark: SparkSession): DataFrame = {

    /** Upsert */
    upsert(output, tableName, updates, uidName)

    /** Compact */
    writeOnce(spark.table(tableName), tableName, output, dataChange = false)
  }

    def scd1(output: String,
             tableName: String,
             updates: DataFrame,
             uidName: String,
             oidName: String,
             createdOnName: String,
             updatedOnName: String)(implicit spark: SparkSession): DataFrame = {

      require(updates.columns.exists(_.equals(uidName)), s"requires column [$uidName]")
      require(updates.columns.exists(_.equals(oidName)), s"requires column [$oidName]")
      require(updates.columns.exists(_.equals(createdOnName)), s"requires column [$createdOnName]")
      require(updates.columns.exists(_.equals(updatedOnName)), s"requires column [$updatedOnName]")

      Try(DeltaTable.forName(tableName)) match {
        case Failure(_) => writeOnce(updates, tableName, output)
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
          writeOnce(spark.table(tableName), tableName, output, dataChange = false)
      }
    }

  def scd1AndCompact(output: String,
                     tableName: String,
                     updates: DataFrame,
                     uidName: String,
                     oidName: String,
                     createdOnName: String,
                     updatedOnName: String)(implicit spark: SparkSession): DataFrame = {
    /** scd1 */
    scd1(output, tableName, updates, uidName, oidName, createdOnName, updatedOnName)
    /** Compact */
    writeOnce(spark.table(tableName), tableName, output, dataChange = false)
  }

    override def writeOnce(df: DataFrame,
                  tableName: String,
                  output: String,
                  repartitionExpr: Seq[Column] = Seq(col("chromosome")),
                  sortWithinPartitions: String = "start",
                  partitionBy: Seq[String] = Seq("chromosome"),
                  dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame = {
      df
        .repartition(1, repartitionExpr: _*)
        .sortWithinPartitions(sortWithinPartitions)
        .write
        .option("dataChange", dataChange)
        .mode(SaveMode.Overwrite)
        .partitionBy(partitionBy: _*)
        .format("delta")
        .option("path", s"$output/$tableName")
        .saveAsTable(s"$tableName")
      df
    }

}
