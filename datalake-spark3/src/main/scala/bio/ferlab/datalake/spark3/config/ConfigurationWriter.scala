package bio.ferlab.datalake.spark3.config

import zio.config._
import zio.config.magnolia.{Descriptor, descriptor}
import zio.config.typesafe._

import java.io.{File, PrintWriter}


object ConfigurationWriter {

  implicit val configDescriptor: Descriptor[Configuration] = Descriptor.getDescriptor[Configuration]

  def toHocon(conf: Configuration): String = {

    val d: ConfigDescriptor[Configuration] = descriptor[Configuration]

    val written: PropertyTree[String, String] = write(d, conf).getOrElse(throw new Exception("bad conf"))
    written.toHoconString
  }

  def writeTo(path: String,
              conf: Configuration): Unit = {

    val content = toHocon(conf)

    println(
      s"""writting configuration: $path :
         |$content
         |""".stripMargin)

    val file = new File(path)
    file.createNewFile()
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()

  }

}
