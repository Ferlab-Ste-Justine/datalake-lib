package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.spark3.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.loader.Format.{DELTA, JDBC}
import bio.ferlab.datalake.spark3.loader.LoadType._
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadResolver {

  type DataSourceLoader = (DatasetConf, DataFrame) => DataFrame

  def resolve(implicit spark: SparkSession, conf: Configuration): PartialFunction[(Format, LoadType), DataSourceLoader] = {
    case (DELTA, Scd1)      => (ds: DatasetConf, df: DataFrame) =>
      val createdOnName = ds.writeoptions.getOrElse(WriteOptions.CREATED_ON_COLUMN_NAME.value, "created_on")
      val updatedOnName = ds.writeoptions.getOrElse(WriteOptions.CREATED_ON_COLUMN_NAME.value, "updated_on")
      DeltaLoader.scd1(ds.location, ds.table.get.database, ds.table.get.name, df, ds.keys, ds.oid, createdOnName, updatedOnName, ds.partitionby, ds.format.sparkFormat)

    case (DELTA, Upsert)    => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.upsert(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.keys, ds.partitionby, ds.format.sparkFormat)

    case (DELTA, OverWrite) => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, dataChange = true)

    case (DELTA, Compact)   => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, dataChange = false)

    case (JDBC, Read)       => (ds: DatasetConf, df: DataFrame) =>
      JdbcLoader.read(ds.location, ds.format.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))

    //generic fallback behaviours
    case (f, OverWrite)   => (ds: DatasetConf, df: DataFrame) =>
      GenericLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, f.sparkFormat)
    case (f, Insert)      => (ds: DatasetConf, df: DataFrame) =>
      GenericLoader.insert(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, f.sparkFormat)
    case (f, Read)        => (ds: DatasetConf, _) =>
      GenericLoader.read(ds.location, f.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))
  }

}
