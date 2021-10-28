package bio.ferlab.datalake.commons.file

import org.slf4j.{Logger, LoggerFactory}

trait FileSystem {

  val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * List all files within a folder.
   *
   * @param path      where to list the files
   * @param recursive if true, list files in a recursive manner
   * @return a list of Files
   */
  def list(path: String, recursive: Boolean): List[File]

  /**
   * Copy the content of a folder or a single file to a destination.
   *
   * @param source      the path to the source folder or file
   * @param destination the path to the destination folder or file
   * @param overwrite   If true, the destination path will be overwritten.
   *                    If false and the destination is not empty then the method will fail.
   */
  def copy(source: String, destination: String, overwrite: Boolean): Unit

  /**
   * Move the content of a folder of a single file to a destination.
   *
   * @param source      the path to the source folder or file
   * @param destination the path to the destination folder or file
   * @param overwrite   If true, the destination path will be overwritten.
   *                    If false and the destination is not empty then the method will fail.
   */
  def move(source: String, destination: String, overwrite: Boolean): Unit

  /**
   * Permanently delete a folder or a file from the system.
   *
   * @param path path to to delete from the system.
   */
  def remove(path: String): Unit

  /**
   * Extract file with a specific extension from a folder
   *
   * @param folder           path to the folder containing the file
   * @param currentExtention extension of the file to look for
   * @param newExtension     extension to set to the file after extraction
   * @return true if the file was extracted, false if the folder does not contain any file with the extension.
   */
  def extractPart(folder: String,
                  currentExtention: String,
                  newExtension: String): Boolean = {

    list(folder, recursive = true)
      .find(_.path.endsWith(s".$currentExtention"))
      .fold(false) { partFile =>

        log.info(s"${partFile.path}")
        log.info(s"-> $folder.$newExtension")

        move(partFile.path, s"$folder.$newExtension", overwrite = true)
        remove(partFile.path)
        true
      }
  }

}
