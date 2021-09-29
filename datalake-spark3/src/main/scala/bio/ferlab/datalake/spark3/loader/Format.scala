package bio.ferlab.datalake.spark3.loader

import enum.Enum

/**
 * List of all [[Format]] supported
 */
object Format {

  /**
   * Apache Avro file format, https://github.com/apache/avro
   */
  case object AVRO extends Format {override val sparkFormat = "avro"; override val fileExtension: String = ".avro"}

  /**
   * Any binary file, can be an image, an audio file or any other type of file.
   */
  case object BINARY extends Format {override val sparkFormat = "binaryFile"; override val fileExtension: String = ".*"}

  /**
   * CSV file or Comma Separated Value file is a common file format for raw files and export file
   */
  case object CSV extends Format {override val sparkFormat = "csv"; override val fileExtension: String = ".csv"}

  /**
   * Variant Call Format https://en.wikipedia.org/wiki/Variant_Call_Format
   */
  case object VCF extends Format {override val sparkFormat = "vcf"; override val fileExtension: String = ".vcf.gz"}

  /**
   * Delta lake format, https://delta.io/
   */
  case object DELTA extends Format {override val sparkFormat = "delta"; override val fileExtension: String = ".parquet"}

  /**
   * Apache Kafka, https://github.com/apache/kafka
   */
  case object KAFKA extends Format {override val sparkFormat = "kafka"; override val fileExtension: String = ".*"}

  /**
   * Json or Javascript object notation
   */
  case object JSON extends Format {override val sparkFormat = "json"; override val fileExtension: String = ".json"}

  /**
   * Derrived from JSON, http://jsonlines.org/
   */
  case object JSON_LINES extends Format {override val sparkFormat = "json"; override val fileExtension: String = ".jsonl"}

  /**
   * Apache Parquet, column oriented data storage format used by (among others) Spark, Impala, Hive, Presto
   * https://en.wikipedia.org/wiki/Apache_Parquet
   */
  case object PARQUET extends Format {override val sparkFormat = "parquet"; override val fileExtension: String = ".parquet"}

  /**
   * Apache ORC, column oriented dat storage used by (among others) Apache Hive, Spark, Nifi
   * https://en.wikipedia.org/wiki/Apache_ORC
   */
  case object ORC extends Format {override val sparkFormat = "orc"; override val fileExtension: String = ".orc"}

  /**
   * Xml
   */
  case object XML extends Format {override val sparkFormat = "xml"; override val fileExtension: String = ".xml"}

  /**
   * Apache Iceberg
   * https://iceberg.apache.org/
   */
  case object ICEBERG extends Format {override val sparkFormat = "iceberg"; override val fileExtension: String = ".parquet"}

  /**
   * Generic JDBC sources like PostgreSQL
   * https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
   */
  case object JDBC extends Format {override val sparkFormat = "jdbc"; override val fileExtension: String = ""}

  /**
   * SQL Server or Azure SQL
   * https://github.com/microsoft/sql-spark-connector
   */
  case object SQL_SERVER extends Format {override val sparkFormat = "com.microsoft.sqlserver.jdbc.spark"; override val fileExtension: String = ""}

  implicit val EnumInstance: Enum[Format] = Enum.derived[Format]
}

/**
 * Sealed trait expressing the definition of a format.
 * Depending on the context it could be the format of a file, a table or a stream like source of data like Kafka.
 * In general, it represents any format that can be read by Spark and loaded as a [[org.apache.spark.sql.Dataset]].
 */
sealed trait Format {
  /**
   * The sparkFormat is the name Spark uses in its API. For a binary file it's "binaryFile".
   */
  val sparkFormat: String

  /**
   * The fileExtension is the extension of the files typically associated to this Format, including the '.'
   * If irrelevant, the fileExtension can be replaced by a wild card.
   */
  val fileExtension: String
}
