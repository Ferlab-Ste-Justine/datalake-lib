package bio.ferlab.datalake.spark3.implicits

import bio.ferlab.datalake.commons.config.LoadType.Read
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.loader.LoadResolver
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      if(LoadResolver.resolve(spark, config).isDefinedAt(ds.format -> Read)) {
        LoadResolver
          .resolve(spark, config)(ds.format -> Read)
          .apply(ds, spark.emptyDataFrame)
      } else {
        throw new NotImplementedError(s"Load is not implemented for [${ds.format} / ${Read}]")
      }
    }
  }

}
