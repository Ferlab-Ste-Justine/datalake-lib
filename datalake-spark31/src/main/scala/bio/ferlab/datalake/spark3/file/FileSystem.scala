package bio.ferlab.datalake.spark3.file

trait FileSystem {

  def list(path: String, recursive: Boolean): List[File]

  def copy(source: String, destination: String, overwrite: Boolean): Unit

  def move(source: String, destination: String, overwrite: Boolean): Unit

  def remove(path: String): Unit

  def extractPart(folder: String,
                  currentExtention: String,
                  newExtension: String): Boolean = {

    list(folder, recursive = true)
      .find(_.path.endsWith(s".$currentExtention"))
      .fold(false){partFile =>

        println(s"${partFile.path}")
        println(s"-> $folder.$newExtension")

        move(partFile.path, s"$folder.$newExtension", overwrite = true)
        remove(partFile.path)
        true
      }
  }

}
