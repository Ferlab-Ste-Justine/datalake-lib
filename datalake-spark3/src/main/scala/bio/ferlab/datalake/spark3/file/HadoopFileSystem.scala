package bio.ferlab.datalake.spark3.file

import bio.ferlab.datalake.commons.file
import bio.ferlab.datalake.commons.file.File
import org.apache.hadoop
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions

object HadoopFileSystem extends file.FileSystem {

  private def getFileSystem(path: String): hadoop.fs.FileSystem = {
    val folderPath = new Path(path)
    val conf = SparkSession.active.sparkContext.hadoopConfiguration
    folderPath.getFileSystem(conf)
  }

  implicit def stringToPath(path: String): Path = new Path(path)

  override def list(path: String, recursive: Boolean): List[File] = {
    val fs = getFileSystem(path)
    val it = fs.listFiles(path, true)
    var files: List[File] = List.empty[File]
    while(it.hasNext) {
      val item = it.next()
      files = files :+ File(item.getPath.toString, item.getPath.getName, item.getLen ,item.isDirectory)
    }
    files
  }

  override def copy(source: String, destination: String, overwrite: Boolean): Unit = {
    val fs = getFileSystem(source)
    fs.copyFromLocalFile(false, overwrite, source, destination)
  }

  override def move(source: String, destination: String, overwrite: Boolean): Unit = {
    val fs = getFileSystem(source)
    fs.copyFromLocalFile(true, overwrite, source, destination)
  }

  override def remove(path: String): Unit = {
    val fs = getFileSystem(path)
    fs.delete(path, true)
  }
}
