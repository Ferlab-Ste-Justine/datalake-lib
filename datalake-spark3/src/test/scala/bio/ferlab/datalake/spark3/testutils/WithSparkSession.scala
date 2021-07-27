package bio.ferlab.datalake.spark3.testutils

import org.apache.commons.io.FileUtils
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


  /**
   * - Creates a temporary folder
   * - executes a function given an output folder absolute path
   * - Clears the output folder after execution
   *
   * @param prefix prefix of the temporary output folder
   * @param block code block to execute
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
}

