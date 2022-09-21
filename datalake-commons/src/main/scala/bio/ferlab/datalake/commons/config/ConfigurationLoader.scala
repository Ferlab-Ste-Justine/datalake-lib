package bio.ferlab.datalake.commons.config

import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.ProductHint

import scala.language.implicitConversions

object ConfigurationLoader {

  implicit def hint[T <: Configuration] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  /**
   * Implicit conversion - convert a Result[Configuration] to a Configuration by loading in succession
   * the file passed in argument, the reference.conf or the application.conf.
   *
   * Throws an exception if no configuration file is found.
   *
   * @param result result from an attempt to load a configuration file
   * @return the configuration file or a fallback configuration.
   */
  def resultToConfiguration[T <: Configuration](result: Result[T])(implicit cr: ConfigReader[T]): T = {
    result
      .fold(_ => ConfigSource.default.load[T], conf => Right(conf))
      .fold(failure => throw new IllegalArgumentException(failure.prettyPrint()), conf => conf)
  }

  /**
   * Load a configuration file from the resources folder
   * eg: config/application.conf
   *
   * @param name the name of the resource
   * @return the configuration loaded as [[Configuration]]
   */
  def loadFromResources[T <: Configuration](name: String)(implicit cr: ConfigReader[T]): T = {
    resultToConfiguration[T](ConfigSource
      .resources(name)
      .load[T]
    )
  }

  /**
   * Load a configuration file from a path
   * eg: file://.../config/application.conf
   *
   * @param path the path where the config file is located.
   * @return the configuration loaded as [[Configuration]]
   */
  def loadFromPath[T <: Configuration](path: String)(implicit cr: ConfigReader[T]): T = {
    resultToConfiguration[T](
      ConfigSource
        .file(path)
        .load[T]
    )
  }

  /**
   * Load a configuration file from a given string
   *
   * @param configString the configuration as a string.
   * @return the configuration loaded as [[Configuration]]
   */
  def loadFromString[T <: Configuration](configString: String)(implicit cr: ConfigReader[T]): T = {
    resultToConfiguration[T](
      ConfigSource
        .string(configString)
        .load[T]
    )
  }
}
