package bio.ferlab.datalake.core.file

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession

object HadoopFileSystem extends FileSystem {
  override def list(path: String, recursive: Boolean): List[File] = {
    val folderPath = new Path(path)
    val conf = SparkSession.active.sparkContext.hadoopConfiguration
    val fs = folderPath.getFileSystem(conf)
    val it = fs.listFiles(folderPath, true)
    var files: List[File] = List()
    while(it.hasNext) {
      val item = it.next()
      files = files :+ File(item.getPath.toString, item.getPath.getName, item.getLen ,item.isDirectory)
    }
    files
  }

  override def copy(source: String, destination: String, overwrite: Boolean): Unit = {
    val src = new Path(source)
    val dst = new Path(destination)
    val conf = SparkSession.active.sparkContext.hadoopConfiguration
    val srcFs = src.getFileSystem(conf)
    srcFs.copyFromLocalFile(false, overwrite, src, dst)
  }

  override def move(source: String, destination: String, overwrite: Boolean): Unit = {
    val src = new Path(source)
    val dst = new Path(destination)
    val conf = SparkSession.active.sparkContext.hadoopConfiguration
    val srcFs = src.getFileSystem(conf)
    srcFs.copyFromLocalFile(true, overwrite, src, dst)
  }

  override def remove(path: String): Unit = {
    val folderPath = new Path(path)
    val conf = SparkSession.active.sparkContext.hadoopConfiguration
    val fs = folderPath.getFileSystem(conf)
    fs.delete(folderPath, true)
  }
}
