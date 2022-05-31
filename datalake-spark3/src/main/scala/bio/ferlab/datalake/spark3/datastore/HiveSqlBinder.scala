package bio.ferlab.datalake.spark3.datastore
import bio.ferlab.datalake.commons.file.FileSystemType
import bio.ferlab.datalake.spark3.file.FileSystemResolver
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object HiveSqlBinder extends SqlBinder {

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * Empty the table and keeps the existing schema and metadata
   *
   * @param location     full path of where the data will be located
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param partitioning list of column used as partition
   * @param format       format of the data to be written
   * @param spark        a valid spark session
   * @return
   */
  override def truncate(location: String,
                        databaseName: String,
                        tableName: String,
                        partitioning: List[String],
                        format: String)(implicit spark: SparkSession): DataFrame = {
    val outputDf = spark.read.format(format).load(location)
      .limit(0)
    outputDf.write
      .mode("overwrite")
      .format(format)
      .save(location)
    outputDf
  }

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
  override def drop(location: String,
                    databaseName: String,
                    tableName: String,
                    fileSystemType: FileSystemType)(implicit spark: SparkSession): DataFrame = {
    FileSystemResolver.resolve(fileSystemType).remove(location)
    spark.sql(s"DROP TABLE IF EXISTS `$databaseName`.`$tableName`")
  }

  /**
   * Set a comment on a field of a table.
   *
   * @param fieldName    the field to update
   * @param fieldType    the field type. [[https://cwiki.apache.org/confluence/display/hive/languagemanual+types#LanguageManualTypes-HiveDataTypes]]
   * @param fieldComment the comment to set
   * @param databaseName the database name
   * @param tableName    the table name
   * @param spark        a valid spark session
   */
  override def setComment(fieldName: String,
                          fieldType: String,
                          fieldComment: String,
                          databaseName: String,
                          tableName: String)(implicit spark: SparkSession): Unit = {
    val stmt = s"""ALTER TABLE $databaseName.$tableName CHANGE $fieldName $fieldName ${fieldType} COMMENT '${fieldComment.take(255)}' """
    Try(spark.sql(stmt)) match {
      case Failure(_) => log.warn(s"sql statement failed: $stmt")
      case Success(_) => log.info(s"updating comment: $stmt")
    }
  }
}
