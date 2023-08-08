package bio.ferlab.datalake.commons.file

object FileSystemResolver {

  def resolve: PartialFunction[FileSystemType, FileSystem] = {
    case _ => HadoopFileSystem
  }

}
