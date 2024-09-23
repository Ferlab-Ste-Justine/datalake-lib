package bio.ferlab.datalake.testutils

import bio.ferlab.datalake.commons.config.SimpleConfiguration
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Path}

trait WithSparkSession {

  private val tmp = new File("tmp").getAbsolutePath
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  /**
   * - Creates a temporary folder
   * - executes a function given an output folder absolute path
   * - Clears the output folder after execution
   *
   * @param prefix prefix of the temporary output folder
   * @param block  code block to execute
   * @tparam T return type of the code block
   * @return the result of the code block
   */
  def withOutputFolder[T](prefix: String)(block: String => T): T = {
    val output: Path = Files.createTempDirectory(prefix)
    try {
      block(output.toAbsolutePath.toString)
    } finally {
      FileUtils.deleteDirectory(output.toFile)
    }
  }

  /**
   * Change configuration storages paths. Can be used in association with `withOutputFolder` to replace all
   * Configuration root paths with the temporary folder.
   *
   * @param conf ETL Configuration
   * @param newPath Path used to replace storages paths
   * @return Updated Configuration with new storages paths
   */
  def updateConfStorages(conf: SimpleConfiguration, newPath: String): SimpleConfiguration = {
    val updatedStorages = conf.storages.map { s => s.copy(path = newPath) }
    conf.copy(datalake = conf.datalake.copy(storages = updatedStorages))
  }
}

