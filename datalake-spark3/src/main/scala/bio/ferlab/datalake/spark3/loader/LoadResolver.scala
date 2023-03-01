package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.{DELTA, ELASTICSEARCH, JDBC, SQL_SERVER}
import bio.ferlab.datalake.commons.config.LoadType._
import bio.ferlab.datalake.commons.config._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

object LoadResolver {

  type DataSourceLoader = (DatasetConf, DataFrame) => DataFrame

  def write(implicit spark: SparkSession, conf: Configuration): PartialFunction[(Format, LoadType), DataSourceLoader] = {
    case (DELTA, Scd1)      => (ds: DatasetConf, df: DataFrame) =>
      val createdOnName = ds.writeoptions.getOrElse(WriteOptions.CREATED_ON_COLUMN_NAME, WriteOptions.DEFAULT_CREATED_ON)
      val updatedOnName = ds.writeoptions.getOrElse(WriteOptions.UPDATED_ON_COLUMN_NAME, WriteOptions.DEFAULT_UPDATED_ON)
      DeltaLoader.scd1(ds.location, ds.table.get.database, ds.table.get.name, df, ds.keys, ds.oid, createdOnName, updatedOnName, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (DELTA, Scd2)      => (ds: DatasetConf, df: DataFrame) =>
      val validFromName = ds.writeoptions.getOrElse(WriteOptions.VALID_FROM_COLUMN_NAME, WriteOptions.DEFAULT_VALID_FROM)
      val validToName = ds.writeoptions.getOrElse(WriteOptions.VALID_TO_COLUMN_NAME, WriteOptions.DEFAULT_VALID_TO)
      val isCurrentName = ds.writeoptions.getOrElse(WriteOptions.IS_CURRENT_COLUMN_NAME, WriteOptions.DEFAULT_IS_CURRENT)
      DeltaLoader.scd2(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.keys, ds.buid, ds.oid, isCurrentName, ds.partitionby, ds.format.sparkFormat, validFromName, validToName, ds.writeoptions)

    case (DELTA, Upsert)    => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.upsert(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.keys, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (DELTA, OverWrite) => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (DELTA, OverWritePartition) => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.overwritePartition(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (DELTA, OverWritePartitionDynamic) => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.overwritePartitionDynamic(ds.location,  ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (DELTA, Compact)   => (ds: DatasetConf, df: DataFrame) =>
      DeltaLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (format, OverWrite) if format == JDBC || format == SQL_SERVER => (ds: DatasetConf, df: DataFrame) =>
      JdbcLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (format, Insert) if format == JDBC || format == SQL_SERVER => (ds: DatasetConf, df: DataFrame) =>
      JdbcLoader.insert(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)

    case (ELASTICSEARCH, OverWrite) => (ds: DatasetConf, df: DataFrame) =>
      ElasticsearchLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(ds.location), df, ds.partitionby, ds.format.sparkFormat, ds.writeoptions)


    //generic fallback behaviours
    case (f, OverWrite)   => (ds: DatasetConf, df: DataFrame) =>
      GenericLoader.writeOnce(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, f.sparkFormat, ds.writeoptions)
    case (f, Insert)      => (ds: DatasetConf, df: DataFrame) =>
      GenericLoader.insert(ds.location, ds.table.map(_.database).getOrElse(""), ds.table.map(_.name).getOrElse(""), df, ds.partitionby, f.sparkFormat, ds.writeoptions)

  }

  def read(implicit spark: SparkSession, conf: Configuration): PartialFunction[Format, DatasetConf => DataFrame] = {

    case format if format == JDBC || format == SQL_SERVER => ds: DatasetConf =>
      JdbcLoader.read(ds.location, format.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))

    case DELTA => ds: DatasetConf =>
      DeltaLoader.read(ds.location, DELTA.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))

    case ELASTICSEARCH => ds: DatasetConf =>
      ElasticsearchLoader.read(ds.location, ELASTICSEARCH.sparkFormat, ds.readoptions, ds.table.map(_.database), ds.table.map(_.name))

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
