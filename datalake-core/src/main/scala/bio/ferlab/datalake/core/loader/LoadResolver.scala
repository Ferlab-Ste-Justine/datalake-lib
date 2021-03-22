package bio.ferlab.datalake.core.loader

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.etl.DataSource
import bio.ferlab.datalake.core.loader.Formats.DELTA
import bio.ferlab.datalake.core.loader.LoadTypes.LoadType
import bio.ferlab.datalake.core.loader.LoadTypes._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadResolver {

  type DataSourceLoader = (DataSource, DataFrame) => DataFrame

  def resolve(implicit spark: SparkSession, conf: Configuration): PartialFunction[(Format, LoadType), DataSourceLoader] = {
    case (DELTA, Upsert)    => (ds: DataSource, df: DataFrame) => DeltaLoader.upsert(ds.location, ds.database, ds.name, df, ds.primaryKeys, ds.partitioning)
    case (DELTA, OverWrite) => (ds: DataSource, df: DataFrame) => DeltaLoader.writeOnce(ds.location, ds.database, ds.name, df, ds.partitioning, dataChange = true)
    case (DELTA, Compact)   => (ds: DataSource, df: DataFrame) => DeltaLoader.writeOnce(ds.location, ds.database, ds.name, df, ds.partitioning, dataChange = false)
  }

}
