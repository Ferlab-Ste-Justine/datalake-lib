package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

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


}

