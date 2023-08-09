package bio.ferlab.datalake.commons.config.testutils

import org.apache.commons.io.FileUtils

import java.nio.file.{Files, Path}

trait WithOutputFolder {

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
}
