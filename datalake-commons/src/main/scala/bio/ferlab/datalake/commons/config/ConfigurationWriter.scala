package bio.ferlab.datalake.commons.config

import com.typesafe.config.ConfigRenderOptions
import org.slf4j
import pureconfig.ConfigWriter


import java.io.{File, PrintWriter}

object ConfigurationWriter {

  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * Write a configuration as HOCON format into a file.
   * @param path path of the resulting file.
   * @param conf configuration to write.
   */
  def writeTo[T <: Configuration](path: String,
              conf: T)(implicit writer: ConfigWriter[T]): Unit = {


    val content: String = toHocon(conf)

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

  def toHocon[T <: Configuration](conf: T)(implicit writer: ConfigWriter[T]): String = {
    val renderOptions = ConfigRenderOptions.defaults().setJson(false).setComments(false).setOriginComments(false).setFormatted(true)
    val content = ConfigWriter[T].to(conf).render(renderOptions)
    content
  }

  /**
   * Remove double quotes around environment variables
   *
   * @param content string on which to apply replace
   * @return the resulting string
   */
  def replaceEnvVariables(content: String): String = content.replaceAll("\"(\\$\\{.*})\"", "$1")
}
