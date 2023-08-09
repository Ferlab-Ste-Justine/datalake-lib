package bio.ferlab.datalake.commons.file

import bio.ferlab.datalake.commons.file
import org.apache.commons.io.IOUtils
import org.apache.hadoop
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.language.implicitConversions

object HadoopFileSystem extends file.FileSystem {

  private def getFileSystem(path: String): hadoop.fs.FileSystem = {
    val folderPath = new Path(path)
    val conf = SparkSession.active.sparkContext.hadoopConfiguration
    folderPath.getFileSystem(conf)
  }

  private def toFile(f: FileStatus): File = File(f.getPath.toString, f.getPath.getName, f.getLen, f.isDirectory)

  implicit def stringToPath(path: String): Path = new Path(path)

  override def list(path: String, recursive: Boolean): List[File] = {
    val fs = getFileSystem(path)

    @tailrec
    def listRecursive(queue: List[Path], acc: List[File]): List[File] = {
      queue match {
        case Nil => acc
        case head :: tail =>
          val statuses = fs.listStatus(head)
          val (dirs, files) = statuses.partition(_.isDirectory)
          val updatedAcc = acc ++ files.map(toFile) ++ dirs.map(toFile)
          listRecursive(tail ++ dirs.map(_.getPath), updatedAcc)
      }
    }

    if (recursive) listRecursive(List(path), Nil)
    else fs.listStatus(path).map(toFile).toList
  }

  override def copy(source: String, destination: String, overwrite: Boolean): Unit = {
    val fs = getFileSystem(source)

    if (fs.exists(destination) && !overwrite) {
      throw new FileAlreadyExistsException("destination " + destination + " already exists")
    } else {
      val inputStream = fs.open(source)
      val outputStream = fs.create(destination)
      IOUtils.copy(inputStream, outputStream)

      inputStream.close()
      outputStream.close()
    }
  }

  override def move(source: String, destination: String, overwrite: Boolean): Unit = {
    copy(source, destination, overwrite)
    remove(source)
  }

  override def remove(path: String): Unit = {
    val fs = getFileSystem(path)
    fs.delete(path, true)
  }
}