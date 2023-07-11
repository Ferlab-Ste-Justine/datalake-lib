package bio.ferlab.datalake.spark3.utils

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

object ResourceLoader {

  def loadResource(path: String): Option[String] = {
    Using(getClass.getClassLoader.getResourceAsStream(path)) { is =>
      if (is == null) None else Some(Source.fromInputStream(is).mkString)
    }.toOption.flatten
  }

}
