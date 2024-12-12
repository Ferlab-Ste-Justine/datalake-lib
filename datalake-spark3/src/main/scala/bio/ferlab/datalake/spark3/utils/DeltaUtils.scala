package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.{ChronoUnit, Temporal}

object DeltaUtils {

  /**
   * @deprecated Use [[DeltaUtils#compact(datasetConf: DatasetConf, partitionFilter: Option[String])]] instead.
   */
  def compact(datasetConf: DatasetConf, repartition: DataFrame => DataFrame)(implicit spark: SparkSession, conf: Configuration): Unit = {
    val df = spark.read
      .format(datasetConf.format.sparkFormat)
      .load(datasetConf.location)
    repartition(df)
      .write
      .partitionBy(datasetConf.partitionby: _*)
      .option("dataChange", "false")
      .format(datasetConf.format.sparkFormat)
      .mode("overwrite")
      .save(datasetConf.location)
  }

  /**
   * Compact the data by coalescing small files into larger ones.
   *
   * @param datasetConf     Dataset to compact
   * @param partitionFilter Optional partition predicate to only compact a subset of data
   * @param spark           Spark session
   * @param conf            Configuration
   * @example
   * Compact the whole dataset.
   * {{{
   *   compact(ds)
   * }}}
   * @example
   * Compact a specific partition. Useful for compaction jobs running everyday on the same dataset.
   * {{{
   *   compact(ds, Some("date='2020-01-01'"))
   * }}}
   */
  def compact(datasetConf: DatasetConf, partitionFilter: Option[String] = None)(implicit spark: SparkSession, conf: Configuration): Unit = {
    val deltaTable = DeltaTable.forPath(datasetConf.location)
    partitionFilter match {
      case Some(pf) => deltaTable.optimize().where(pf).executeCompaction()
      case None => deltaTable.optimize().executeCompaction()
    }
  }

  /**
   * Vacuum based on the number of versions we wants keep. Notes :
   * - If there is versions younger than 2 weeks then these versions will be kept and the retention period will be set to 336 hours (2 weeks)
   * - If there is less versions than numberOfVersions param then vacuum will not be executed
   *
   * @param datasetConf      dataset to vacuum
   * @param numberOfVersions number of versions to kept
   * @param spark            spark session
   * @param conf             conf
   */
  def vacuum(datasetConf: DatasetConf, numberOfVersions: Int)(implicit spark: SparkSession, conf: Configuration): Unit = {
    import spark.implicits._
    val timestamps: Seq[Timestamp] = DeltaTable
      .forPath(datasetConf.location)
      .history(numberOfVersions)
      .select("timestamp")
      .as[Timestamp].collect().toSeq
    if (timestamps.size == numberOfVersions) {
      val retentionHours = Seq(336, getRetentionHours(timestamps)).max // 336 hours = 2 weeks
      DeltaTable.forPath(datasetConf.location).vacuum(retentionHours)
    }

  }

  def getRetentionHours(timestamps: Seq[Timestamp], clock: Temporal = LocalDateTime.now()): Long = {
    val oldest = timestamps.min((x: Timestamp, y: Timestamp) => if (x.before(y)) -1 else if (x.after(y)) 1 else 0)
    oldest.toLocalDateTime.minusHours(1).until(clock, ChronoUnit.HOURS)
  }

  /**
   * Retrieves statistics per file for the specified Delta table.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A DataFrame with the following schema:
   *         - `path`: Path of the file.
   *         - `partitionValues`: Map of partition column names to their values.
   *         - `size`: Size of the file in bytes.
   *         - `modificationTime`: Last modification timestamp.
   *         - `dataChange`: Indicates whether the file represents a data change.
   *         - `tags`: Map of additional metadata tags.
   *         - `stats`: Nested struct with:
   *           - `numRecords`: Number of records in the file.
   *           - `minValues`: Minimum values for columns.
   *           - `maxValues`: Maximum values for columns.
   *           - `nullCount`: Count of null values for columns.
   * @example Result schema example:
   * {{{
   * root
   *  |-- path: string (nullable = true)
   *  |-- partitionValues: map (nullable = true)
   *  |    |-- key: string
   *  |    |-- value: string (valueContainsNull = true)
   *  |-- size: long (nullable = false)
   *  |-- modificationTime: long (nullable = false)
   *  |-- dataChange: boolean (nullable = false)
   *  |-- stats: struct (nullable = true)
   *  |    |-- numRecords: long (nullable = true)
   *  |    |-- minValues: struct (nullable = true)
   *  |    |    |-- AA_ID: decimal(14,0) (nullable = true)
   *  |    |    |-- ID: string (nullable = true)
   *  |    |-- maxValues: struct (nullable = true)
   *  |    |    |-- AA_ID: decimal(14,0) (nullable = true)
   *  |    |    |-- ID: string (nullable = true)
   *  |    |-- nullCount: struct (nullable = true)
   *  |    |    |-- AA_ID: long (nullable = true)
   *  |    |    |-- ID: long (nullable = true)
   *  |-- tags: map (nullable = true)
   *  |    |-- key: string
   *  |    |-- value: string (valueContainsNull = true)
   * }}}
   */
  def getTableStats(path: String)(implicit spark: SparkSession): DataFrame = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, path)
    snapshot.withStats
  }

  private def getStatPerPartition(stats: DataFrame, statAggregations: Column*): DataFrame = {
    stats
      .select(
        explode(col("partitionValues")) as Seq("partitionColumn", "partitionValue"),
        stats("*")
      )
      .groupBy("partitionColumn", "partitionValue")
      .agg(statAggregations.head, statAggregations.tail: _*)
  }

  /**
   * Extracts partition column names and their distinct sorted values from the Delta table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A DataFrame with columns:
   *         - `partitionColumn`: Name of the partition columns.
   *         - `values`: A sorted sequence of distinct values for the partition column.
   * @example Result DataFrame example:
   * {{{
   * +---------------+-----------------------------------+
   * |partitionColumn|values                             |
   * +---------------+-----------------------------------+
   * |ORIGIN         |[ORDER, PATIENT, STAY, TEST_RESULT]|
   * |ingested_on_dte|[2024-11-26, 2024-11-27]           |
   * +---------------+-----------------------------------+
   * }}}
   */
  def getPartitionValues(path: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    getTableStats(path)
      .select(explode($"partitionValues") as Seq("partitionColumn", "value"))
      .groupBy("partitionColumn")
      .agg(array_sort(collect_set("value")) as "values")
  }

  /**
   * Computes the total number of records in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return The total number of records in the table.
   */
  def getNumRecords(path: String)(implicit spark: SparkSession): Long = {
    import spark.implicits._

    getTableStats(path)
      .select(sum("stats.numRecords"))
      .as[Long]
      .head()
  }

  /**
   * Computes the total number of records for each partition value in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A DataFrame with columns:
   *         - `partitionColumn`: Name of the partition column.
   *         - `partitionValue`: Value of the partition.
   *         - `numRecords`: Total number of records for the partition value.
   * @example Result DataFrame example:
   * {{{
   * +---------------+--------------+----------+
   * |partitionColumn|partitionValue|numRecords|
   * +---------------+--------------+----------+
   * |ORIGIN         |ORDER         |68636088  |
   * |ORIGIN         |TEST_RESULT   |22787146  |
   * |ORIGIN         |PATIENT       |104806    |
   * |ORIGIN         |STAY          |2024526   |
   * |ingested_on_dte|2024-11-26    |46776282  |
   * |ingested_on_dte|2024-11-27    |46776284  |
   * +---------------+--------------+----------+
   * }}}
   */
  def getNumRecordsPerPartition(path: String)(implicit spark: SparkSession): DataFrame = {
    val numRecords = sum("stats.numRecords") as "numRecords"
    getStatPerPartition(getTableStats(path), numRecords)
  }

  /**
   * Computes the minimum value for each non-partition column in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A Map where:
   *         - Keys are column names.
   *         - Values are the minimum values observed in the corresponding columns.
   * @example Example result:
   * {{{
   * Map(
   *   "ID" -> 1,
   *   "UPDATE_DATE" -> 1900-01-01,
   * )
   * }}}
   */
  def getMinValues(path: String)(implicit spark: SparkSession): Map[String, Any] = {
    val statsDf = getTableStats(path).select("stats.minValues.*")
    val columnNames = statsDf.columns
    val minValues = columnNames.map(c => min(c) as c)

    statsDf
      .select(minValues: _*)
      .head()
      .getValuesMap[Any](columnNames)
  }

  /**
   * Computes the minimum value for each column for each partition value in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A DataFrame with columns:
   *         - `partitionColumn`: Name of the partition column.
   *         - `partitionValue`: Value of the partition.
   *         - Each column in the Delta table: The minimum value for each column.
   * @example For the given Delta table:
   * {{{
   * +----------+---------+-----------+--------+-------------------+
   * | AA_ID    | ID      | UPDATE_DATE| ORIGIN | ingested_on_dte  |
   * +----------+---------+-----------+--------+-------------------+
   * | 10001    | 'ABC123'| 2024-01-01 | ORDER  | 2024-11-26       |
   * | 10002    | 'XYZ456'| 2024-01-02 | ORDER  | 2024-11-27       |
   * +----------+---------+-----------+--------+-------------------+
   * }}}
   *
   *          The result DataFrame would look like:
   * {{{
   * +---------------+--------------+------------+------------+------------+
   * |partitionColumn|partitionValue|AA_ID       |ID          |UPDATE_DATE |
   * +---------------+--------------+------------+------------+------------+
   * |ORIGIN         |ORDER         |10001       |'ABC123'    |2024-01-01  |
   * |ingested_on_dte|2024-11-26    |10001       |'ABC123'    |2024-01-01  |
   * |ingested_on_dte|2024-11-27    |10002       |'XYZ456'    |2024-01-02  |
   * +---------------+--------------+------------+------------+------------+
   * }}}
   */
  def getMinValuesPerPartition(path: String)(implicit spark: SparkSession): DataFrame = {
    val statsDf = getTableStats(path)
    val columnNames = statsDf.select("stats.minValues.*").columns
    val minValues = columnNames.map(c => min(s"stats.minValues.$c") as c)

    getStatPerPartition(statsDf, minValues: _*)
  }

  /**
   * Computes the maximum value for each non-partition column in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A Map where:
   *         - Keys are column names.
   *         - Values are the maximum values observed in the corresponding columns.
   * @example Example result:
   * {{{
   * Map(
   *   "ID" -> 9999,
   *   "UPDATE_DATE" -> 2024-01-01,
   * )
   * }}}
   */
  def getMaxValues(path: String)(implicit spark: SparkSession): Map[String, Any] = {
    val statsDf = getTableStats(path).select("stats.maxValues.*")
    val columnNames = statsDf.columns
    val maxValues = columnNames.map(c => max(c) as c)

    statsDf
      .select(maxValues: _*)
      .head()
      .getValuesMap[Any](columnNames)
  }

  /**
   * Computes the maximum value for each column for each partition value in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A DataFrame with columns:
   *         - `partitionColumn`: Name of the partition column.
   *         - `partitionValue`: Value of the partition.
   *         - Each column in the Delta table: The maximum value for each column.
   * @example For the given Delta table:
   * {{{
   * +----------+---------+-----------+--------+-------------------+
   * | AA_ID    | ID      | UPDATE_DATE| ORIGIN | ingested_on_dte  |
   * +----------+---------+-----------+--------+-------------------+
   * | 10001    | 'ABC123'| 2024-01-01 | ORDER  | 2024-11-26       |
   * | 10002    | 'XYZ456'| 2024-01-02 | ORDER  | 2024-11-27       |
   * +----------+---------+-----------+--------+-------------------+
   * }}}
   *
   *          The result DataFrame would look like:
   * {{{
   * +---------------+--------------+------------+------------+------------+
   * |partitionColumn|partitionValue|AA_ID       |ID          |UPDATE_DATE |
   * +---------------+--------------+------------+------------+------------+
   * |ORIGIN         |ORDER         |10002       |'XYZ456'    |2024-01-02  |
   * |ingested_on_dte|2024-11-26    |10001       |'ABC123'    |2024-01-01  |
   * |ingested_on_dte|2024-11-27    |10002       |'XYZ456'    |2024-01-02  |
   * +---------------+--------------+------------+------------+------------+
   * }}}
   */
  def getMaxValuesPerPartition(path: String)(implicit spark: SparkSession): DataFrame = {

    val statsDf = getTableStats(path)
    val columnNames = statsDf.select("stats.maxValues.*").columns
    val maxValues = columnNames.map(c => max(s"stats.maxValues.$c") as c)

    getStatPerPartition(statsDf, maxValues: _*)
  }

  /**
   * Computes the total number of nulls for each non-partition column in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A Map where:
   *         - Keys are column names.
   *         - Values are the number of nulls observed in the corresponding columns.
   * @example Example result:
   * {{{
   * Map(
   *   "ID" -> 0,
   *   "UPDATE_DATE" -> 256,
   * )
   * }}}
   */
  def getNullCounts(path: String)(implicit spark: SparkSession): Map[String, Long] = {
    val statsDf = getTableStats(path).select("stats.nullCount.*")
    val columnNames = statsDf.columns
    val nullCounts = columnNames.map(c => sum(c) as c)

    statsDf
      .select(nullCounts: _*)
      .head()
      .getValuesMap[Long](columnNames)
  }

  /**
   * Computes the total number of nulls for each column for each partition value in a Delta table using the table's metadata.
   *
   * @param path  Path of the Delta table
   * @param spark Spark session
   * @return A DataFrame with columns:
   *         - `partitionColumn`: Name of the partition column.
   *         - `partitionValue`: Value of the partition.
   *         - Each column in the Delta table: The number of nulls for each column.
   * @example For the given Delta table:
   * {{{
   * +----------+---------+------------+--------+------------------+
   * | AA_ID    | ID      | UPDATE_DATE| ORIGIN | ingested_on_dte  |
   * +----------+---------+------------+--------+------------------+
   * | 10001    | 'ABC123'| null       | ORDER  | 2024-11-26       |
   * | 10002    | null    | null       | ORDER  | 2024-11-27       |
   * +----------+---------+------------+--------+------------------+
   * }}}
   *
   *          The result DataFrame would look like:
   * {{{
   * +---------------+--------------+-----+---+------------+
   * |partitionColumn|partitionValue|AA_ID|ID |UPDATE_DATE |
   * +---------------+--------------+-----+---+------------+
   * |ORIGIN         |ORDER         |0    |1  |2           |
   * |ingested_on_dte|2024-11-26    |0    |0  |1           |
   * |ingested_on_dte|2024-11-27    |0    |1  |1           |
   * +---------------+--------------+-----+---+------------+
   * }}}
   */
  def getNullCountsPerPartition(path: String)(implicit spark: SparkSession): DataFrame = {

    val statsDf = getTableStats(path)
    val columnNames = statsDf.select("stats.nullCount.*").columns
    val nullCounts = columnNames.map(c => sum(s"stats.nullCount.$c") as c)

    getStatPerPartition(statsDf, nullCounts: _*)
  }
}

