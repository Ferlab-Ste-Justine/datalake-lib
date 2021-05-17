package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.loader.Format.DELTA
import bio.ferlab.datalake.spark3.loader.LoadType._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadResolver {

  type DataSourceLoader = (DatasetConf, DataFrame) => DataFrame

  def resolve(implicit spark: SparkSession, conf: Configuration): PartialFunction[(Format, LoadType), DataSourceLoader] = {
    case (DELTA, Upsert)    => (ds: DatasetConf, df: DataFrame) => DeltaLoader.upsert(ds.location, ds.table.get.database, ds.table.get.name, df, ds.keys, ds.partitionby)
    case (DELTA, OverWrite) => (ds: DatasetConf, df: DataFrame) => DeltaLoader.writeOnce(ds.location, ds.table.get.database, ds.table.get.name, df, ds.partitionby, dataChange = true)
    case (DELTA, Compact)   => (ds: DatasetConf, df: DataFrame) => DeltaLoader.writeOnce(ds.location, ds.table.get.database, ds.table.get.name, df, ds.partitionby, dataChange = false)
  }

}
