package bio.ferlab.datalake.core.file

trait FileSystem {

  def list(path: String, recursive: Boolean): List[File]

  def copy(source: String, destination: String, overwrite: Boolean): Unit

  def move(source: String, destination: String, overwrite: Boolean): Unit

  def remove(path: String): Unit

}

case class File(path: String, name: String, size: Long, isDir: Boolean)
