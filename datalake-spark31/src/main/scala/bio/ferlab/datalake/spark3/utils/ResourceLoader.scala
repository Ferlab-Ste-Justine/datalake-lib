package bio.ferlab.datalake.spark3.utils

import scala.io.Source
import scala.util.{Failure, Success, Using}

object ResourceLoader {
  
  def loadResource(path: String): String = {
    Using(getClass.getClassLoader.getResourceAsStream(path)) { is =>
      Source.fromInputStream(is).mkString
    } match {
      case Success(content) => content
      case Failure(exception) => throw new RuntimeException(s"Failed to load resource: $path", exception)
    }
  }

}
