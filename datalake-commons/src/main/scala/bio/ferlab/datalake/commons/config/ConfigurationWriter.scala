package bio.ferlab.datalake.commons.config

import org.slf4j
import zio.config._
import zio.config.magnolia.{Descriptor, descriptor}
import zio.config.typesafe._

import java.io.{File, PrintWriter}

object ConfigurationWriter {

  implicit val configDescriptor: Descriptor[Configuration] = Descriptor.getDescriptor[Configuration]
  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def toHocon(conf: Configuration): String = {

    val d: ConfigDescriptor[Configuration] = descriptor[Configuration]

    val written: PropertyTree[String, String] = write(d, conf).getOrElse(throw new Exception("bad conf"))
    written.toHoconString
  }

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
