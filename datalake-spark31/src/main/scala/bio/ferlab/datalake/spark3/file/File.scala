package bio.ferlab.datalake.spark3.file

case class File(path: String, name: String, size: Long, isDir: Boolean)
