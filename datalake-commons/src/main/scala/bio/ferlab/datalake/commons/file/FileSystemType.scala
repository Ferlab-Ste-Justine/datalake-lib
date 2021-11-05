package bio.ferlab.datalake.commons.file

import enum.Enum

sealed trait FileSystemType

/**
 * List of all FileSystemTypes supported
 */
object FileSystemType {

  /**
   * Managed files systems
   */
  case object ADLS extends FileSystemType
  case object GS extends FileSystemType
  case object HDFS extends FileSystemType
  case object LOCAL extends  FileSystemType
  case object S3 extends FileSystemType

  /**
   * Unmanaged file systems - systems for which the application does not manage the file system.
   * Example of unmanaged systems a MariaDB, MongoDB, Elasticsearch.
   */
  case object UNMANAGED extends FileSystemType


  implicit val EnumInstance: Enum[FileSystemType] = Enum.derived[FileSystemType]
}