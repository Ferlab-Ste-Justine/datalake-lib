package bio.ferlab.datalake.commons.config

import org.slf4j
import zio.config._
import zio.config.magnolia.{Descriptor, descriptor}
import zio.config.typesafe._

import java.io.{File, PrintWriter}

object ConfigurationWriter {

//  implicit val configDescriptor: Descriptor[SimpleConfiguration] = Descriptor.getDescriptor[SimpleConfiguration]
  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * Convert a Configuration into a HOCON string. More information on the format here: https://github.com/lightbend/config.
   * @param conf configuration to convert
   * @return a configuration as HOCON string
   */
  def toHocon[T <: Configuration](conf: T)(implicit configDescriptor: Descriptor[T]): String = {

    val d: ConfigDescriptor[T] = descriptor[T]

    val written: PropertyTree[String, String] = write(d, conf).getOrElse(throw new Exception("bad conf"))
    written.toHoconString
  }

  /**
   * Write a configuration as HOCON format into a file.
   * @param path path of the resulting file.
   * @param conf configuration to write.
   */
  def writeTo[T <: Configuration](path: String,
              conf: T)(implicit configDescriptor: Descriptor[T]): Unit = {

    val content:String = toHocon[T](conf)

    val contentWithEnvVariable = replaceEnvVariables(content)

    log.debug(
      s"""writting configuration: $path :
         |$contentWithEnvVariable
         |""".stripMargin)

    val file = new File(path)
    file.createNewFile()
    val pw = new PrintWriter(file)
    pw.write(contentWithEnvVariable)
    pw.close()
  }

  /**
   * Remove double quotes around environment variables
   *
   * @param content string on which to apply replace
   * @return the resulting string
   */
  def replaceEnvVariables(content: String): String = content.replaceAll("\"(\\$\\{.*})\"", "$1")
}
