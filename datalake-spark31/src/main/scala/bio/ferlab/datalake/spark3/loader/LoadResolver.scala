package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, Format, LoadType, WriteOptions}
import bio.ferlab.datalake.commons.config.Format.{DELTA, JDBC, SQL_SERVER}
import bio.ferlab.datalake.commons.config.LoadType._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

object LoadResolver {

  type DataSourceLoader = (DatasetConf, DataFrame) => DataFrame

  def write(implicit spark: SparkSession, conf: Configuration): PartialFunction[(Format, LoadType), DataSourceLoader] = {
    case (DELTA, Scd1)      => (ds: DatasetConf, df: DataFrame) =>
      val createdOnName = ds.writeoptions.getOrElse(WriteOptions.CREATED_ON_COLUMN_NAME.value, "created_on")
      val updatedOnName = ds.writeoptions.getOrElse(WriteOptions.CREATED_ON_COLUMN_NAME.value, "updated_on")
      DeltaLoader.scd1(ds.location, ds.table.get.database, ds.table.get.name, df, ds.keys, ds.oid, createdOnName, updatedOnName, ds.partitionby, ds.format.sparkFormat)

    case (DELTA, Upsert)    => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.upsert(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.keys, ds.partitionby, ds.format.sparkFormat)

    case (DELTA, OverWrite) => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (DELTA, OverWritePartition) => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.overwritePartition(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)


    case (DELTA, Compact)   => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)


    //generic fallback behaviours
    case (f, OverWrite)   => (ds: DatasetConf, df: DataFrame) =>
      GenericLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, f.sparkFormat, ds.writeoptions)
    case (f, Insert)      => (ds: DatasetConf, df: DataFrame) =>
      GenericLoader.insert(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, f.sparkFormat)

  }

  def read(implicit spark: SparkSession, conf: Configuration): PartialFunction[Format, DatasetConf => DataFrame] = {

    case format if format == JDBC || format == SQL_SERVER => ds: DatasetConf =>
      JdbcLoader.read(ds.location, format.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))

    case DELTA => ds: DatasetConf =>
      DeltaLoader.read(ds.location, DELTA.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))

    case format => ds: DatasetConf =>
      GenericLoader.read(ds.location, format.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))
  }

  def resetTo(implicit spark: SparkSession, conf: Configuration): PartialFunction[(Format, LoadType), (LocalDateTime, DatasetConf) => Unit] = {
    case (f, Scd2) => (_, ds) =>
      val df = read(spark, conf)(f).apply(ds).limit(0)
      write(spark, conf)(f, OverWrite).apply(ds, df)

    case (f, _) => (dt, ds) =>
      val df = read(spark, conf)(f).apply(ds).limit(0)
      write(spark, conf)(f, OverWrite).apply(ds, df)


  }

}
