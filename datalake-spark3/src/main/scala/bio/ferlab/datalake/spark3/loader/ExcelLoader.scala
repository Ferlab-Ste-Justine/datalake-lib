package bio.ferlab.datalake.spark3.loader

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate

object ExcelLoader extends Loader {

  override def read(location: String,
                    format: String,
                    readOptions: Map[String, String],
                    databaseName: Option[String] = None,
                    tableName: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    require(readOptions.isDefinedAt("header"), "Expecting [header] to be defined in readOptions.")

    spark
      .read
      .format(format)
      .options(readOptions)
      .load(location)
  }

  def write(df: DataFrame,
            location: String,
            databaseName: String,
            tableName: String,
            partitioning: List[String],
            format: String,
            options: Map[String, String],
            mode: SaveMode): DataFrame = {
    // Excel format requires the schema to be non-empty, does not support empty schema dataframe writes
    require(df.schema.nonEmpty, "DataFrame must have a valid schema with at least one column.")

    df.write
      .options(options)
      .format(format)
      .mode(mode)
      .save(location)
    df
  }

  override def overwritePartition(location: String,
                                  databaseName: String,
                                  tableName: String,
                                  df: DataFrame,
                                  partitioning: List[String],
                                  format: String,
                                  options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

  override def writeOnce(location: String,
                         databaseName: String,
                         tableName: String,
                         df: DataFrame,
                         partitioning: List[String],
                         format: String,
                         options: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    write(df, location, databaseName, tableName, partitioning, format, options, SaveMode.Overwrite)
  }

  override def insert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    write(updates, location, databaseName, tableName, partitioning, format, options, SaveMode.Append)
  }

  override def upsert(location: String,
                      databaseName: String,
                      tableName: String,
                      updates: DataFrame,
                      primaryKeys: Seq[String],
                      partitioning: List[String],
                      format: String,
                      options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

  override def scd1(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    oidName: String,
                    createdOnName: String,
                    updatedOnName: String,
                    partitioning: List[String],
                    format: String,
                    options: Map[String, String])(implicit spark: SparkSession): DataFrame = ???

  override def scd2(location: String,
                    databaseName: String,
                    tableName: String,
                    updates: DataFrame,
                    primaryKeys: Seq[String],
                    buidName: String,
                    oidName: String,
                    isCurrentName: String,
                    partitioning: List[String],
                    format: String,
                    validFromName: String,
                    validToName: String,
                    options: Map[String, String],
                    minValidFromDate: LocalDate,
                    maxValidToDate: LocalDate)(implicit spark: SparkSession): DataFrame = ???
}
