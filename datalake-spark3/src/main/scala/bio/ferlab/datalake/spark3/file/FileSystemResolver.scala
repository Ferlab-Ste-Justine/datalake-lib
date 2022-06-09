package bio.ferlab.datalake.spark3.file

import bio.ferlab.datalake.commons.file.{FileSystem, FileSystemType}

object FileSystemResolver {

  def resolve: PartialFunction[FileSystemType, FileSystem] = {
    case _ => HadoopFileSystem
  }

}
