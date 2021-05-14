package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf}
import bio.ferlab.datalake.spark3.loader.Format.DELTA
import bio.ferlab.datalake.spark3.loader.LoadType._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadResolver {

  type DataSourceLoader = (SourceConf, DataFrame) => DataFrame

  def resolve(implicit spark: SparkSession, conf: Configuration): PartialFunction[(Format, LoadType), DataSourceLoader] = {
    case (DELTA, Upsert)    => (ds: SourceConf, df: DataFrame) => DeltaLoader.upsert(ds.location, ds.database, ds.name, df, ds.keys, ds.partitionby)
    case (DELTA, OverWrite) => (ds: SourceConf, df: DataFrame) => DeltaLoader.writeOnce(ds.location, ds.database, ds.name, df, ds.partitionby, dataChange = true)
    case (DELTA, Compact)   => (ds: SourceConf, df: DataFrame) => DeltaLoader.writeOnce(ds.location, ds.database, ds.name, df, ds.partitionby, dataChange = false)
  }

}
