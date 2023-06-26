package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.commons.config.LoadType.{OverWritePartition, Scd2}
import bio.ferlab.datalake.commons.config.WriteOptions.IS_CURRENT_COLUMN_NAME
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

object DatasetConfImplicits {

  implicit class DatasetConfOperations(ds: DatasetConf) {

    /**
     * Using an instance of Spark and the current configuration, reads the dataset from either the tableName or from the
     * location.
     *
     * @param config configuration currently loaded
     * @param spark  instance of SparkSession
     * @return
     */
    def read(implicit config: Configuration, spark: SparkSession): DataFrame = {
      if (LoadResolver.read(spark, config).isDefinedAt(ds.format)) {
        LoadResolver
          .read(spark, config)(ds.format)
          .apply(ds)
      } else {
        throw new NotImplementedError(s"Read is not implemented for [${ds.format}]")
      }
    }

    /**
     * Read current version of a given DatasetConf based on its loadType
     *
     * @param config configuration currently loaded
     * @param spark  instance of SparkSession
     * @return
     */
    def readCurrent(implicit config: Configuration, spark: SparkSession): DataFrame = {
      val df = ds.read
      ds.loadtype match {
        case OverWritePartition =>
          val maxPartition = df.select(max(col(ds.partitionby.head))).collect().head.get(0)
          df.filter(col(ds.partitionby.head) === maxPartition)
        case Scd2 =>
          df.filter(col(ds.writeoptions(IS_CURRENT_COLUMN_NAME)))
        case _ => df
      }
    }

    def resetTo(dateTime: LocalDateTime)(implicit config: Configuration, spark: SparkSession): Unit = {
      if (LoadResolver.resetTo(spark, config).isDefinedAt(ds.format, ds.loadtype)) {
        LoadResolver
          .resetTo(spark, config)(ds.format, ds.loadtype)
          .apply(dateTime, ds)
      } else {
        throw new NotImplementedError(s"Reset is not implemented for [${ds.format} / ${ds.loadtype}]")
      }

    }

    def tableExist(implicit spark: SparkSession): Boolean = {
      ds.table.exists(t => spark.catalog.tableExists(t.fullName))
    }

    /**
     * Replace the substrings matching the pattern with the replacement value in the path.
     *
     * @param pattern     The sequence to be replaced
     * @param replacement The replacement sequence
     * @return A new [[DatasetConf]] with its path updated
     */
    def replacePath(pattern: String, replacement: String): DatasetConf = {
      val newPath = ds.path.replace(pattern, replacement)
      ds.copy(path = newPath)
    }

    /**
     * Replace the substrings matching the pattern with the replacement value in the table name. Valid table names
     * only contain alphabet characters, numbers and _. If clean is set to true, all invalid characters will be replaced
     * by an _. The method will not fail when using a replacement value with invalid characters, but Spark might fail
     * later when trying to create the table.
     *
     * @param pattern     The sequence to be replaced
     * @param replacement The replacement sequence
     * @param clean       If true, cleans the replacement sequence
     * @return A new [[DatasetConf]] with its table name updated
     */
    def replaceTableName(pattern: String, replacement: String, clean: Boolean): DatasetConf = {
      val table = ds.table
      val cleanedReplacement = if (clean) {
        val invalidCharacters = "[^a-zA-Z0-9_]".r
        invalidCharacters.replaceAllIn(replacement, "_")
      } else replacement
      if (table.isDefined) {
        val newTableName = table.get.name.replace(pattern, cleanedReplacement)
        ds.copy(table = Some(table.get.copy(name = newTableName)))
      } else ds
    }

    /**
     * Replace the placeholders in the [[DatasetConf]] with the replacement value. The replacement value will be
     * cleaned before being used in the table name. Valid table names only contain alphabet characters, numbers and _.
     * All invalid characters will be replaced by an _.
     *
     * @param placeholder The placeholder value to be replaced
     * @param replacement The replacement sequence
     * @return A new [[DatasetConf]] with its placeholders filled
     */
    def replacePlaceholders(placeholder: String, replacement: String): DatasetConf = {
      ds.replacePath(placeholder, replacement)
        .replaceTableName(placeholder, replacement, clean = true)
    }
  }
}
