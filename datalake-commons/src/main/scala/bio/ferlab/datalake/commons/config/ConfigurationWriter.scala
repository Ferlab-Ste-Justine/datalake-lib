package bio.ferlab.datalake.commons.config

import org.slf4j
import zio.config._
import zio.config.magnolia.{Descriptor, descriptor}
import zio.config.typesafe._

import java.io.{File, PrintWriter}

object ConfigurationWriter {

  implicit val configDescriptor: Descriptor[Configuration] = Descriptor.getDescriptor[Configuration]
  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * Convert a Configuration into a HOCON string. More information on the format here: https://github.com/lightbend/config.
   * @param conf configuration to convert
   * @return a configuration as HOCON string
   */
  def toHocon(conf: Configuration): String = {

    val d: ConfigDescriptor[Configuration] = descriptor[Configuration]

    val written: PropertyTree[String, String] = write(d, conf).getOrElse(throw new Exception("bad conf"))
    written.toHoconString
  }

  /**
   * Write a configuration as HOCON format into a file.
   * @param path path of the resulting file.
   * @param conf configuration to write.
   */
  def writeTo(path: String,
              conf: Configuration): Unit = {

    val content = toHocon(conf)

    val contentWithEnvVariable = content
      .replaceAll("\"\\$\\{", "\\$\\{")
      .replaceAll("}\"", "}")

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

}
