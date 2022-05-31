package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.commons.config.LoadType.Read
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

object DatasetConfImplicits {

  implicit class DatasetConfOperations(ds: DatasetConf) {

    /**
     * Using an instance of Spark and the current configuration, reads the dataset from either the tableName or from the
     * location.
     * @param config configuration currently loaded
     * @param spark instance of SparkSession
     * @return
     */
    def read(implicit config: Configuration, spark: SparkSession): DataFrame = {
      if(LoadResolver.read(spark, config).isDefinedAt(ds.format)) {
        LoadResolver
          .read(spark, config)(ds.format)
          .apply(ds)
      } else {
        throw new NotImplementedError(s"Read is not implemented for [${ds.format}]")
      }
    }

    def resetTo(dateTime: LocalDateTime)(implicit config: Configuration, spark: SparkSession): Unit = {
      if(LoadResolver.resetTo(spark, config).isDefinedAt(ds.format, ds.loadtype)) {
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
  }


}
