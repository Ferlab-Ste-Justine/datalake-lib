package bio.ferlab.datalake.spark3.datastore

import bio.ferlab.datalake.commons.file.FileSystemType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SqlBinder {
  /**
   * Empty the table and keeps the existing schema and metadata
   * @param location full path of where the data will be located
   * @param databaseName the name of the database
   * @param tableName the name of the table
   * @param partitioning list of column used as partition
   * @param format format of the data to be written
   * @param spark a valid spark session
   * @return
   */
  def truncate(location: String,
               databaseName: String,
               tableName: String,
               partitioning: List[String],
               format: String)(implicit spark: SparkSession): DataFrame

  /**
   * Empty the table and removes the associated metadata
   *
   * @param location       full path of where the data will be located
   * @param databaseName   the name of the database
   * @param tableName      the name of the table
   * @param fileSystemType the type of file system
   * @param spark          a valid spark session
   * @return
   */
  def drop(location: String,
           databaseName: String,
           tableName: String,
           fileSystemType: FileSystemType)(implicit spark: SparkSession): DataFrame

  /**
   * Set a comment on a field of a table.
   * @param fieldName the field to update
   * @param fieldType the field type. [[https://cwiki.apache.org/confluence/display/hive/languagemanual+types#LanguageManualTypes-HiveDataTypes]]
   * @param fieldComment the comment to set
   * @param databaseName the database name
   * @param tableName the table name
   * @param spark a valid spark session
   */
  def setComment(fieldName: String,
                 fieldType: String,
                 fieldComment: String,
                 databaseName: String,
                 tableName: String)(implicit spark: SparkSession): Unit
}
