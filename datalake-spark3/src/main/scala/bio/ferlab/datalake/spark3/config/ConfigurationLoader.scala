package bio.ferlab.datalake.spark3.config

import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enum._

import scala.language.implicitConversions

object ConfigurationLoader {


  /**
   * Implicit conversion - convert a Result[Configuration] to a Configuration by loading in succession
   * the file passed in argument, the reference.conf or the application.conf.
   *
   * Throws an exception if no configuration file is found.
   *
   * @param result result from an attempt to load a configuration file
   * @return the configuration file or a fallback configuration.
   */
  implicit def resultToConfiguration(result: Result[Configuration]): Configuration = {
    result
      .fold(_ => ConfigSource.default.load[Configuration], conf => Right(conf))
      .fold(failure => throw new IllegalArgumentException(failure.prettyPrint()), conf => conf)
  }

  /**
   * Load a configuration file from the resources folder
   * eg: config/application.conf
   *
   * @param name the name of the resource
   * @return the configuration loaded as [[Configuration]]
   */
  def loadFromResources(name: String): Configuration = {
    ConfigSource
      .resources(name)
      .load[Configuration]
  }

  /**
   * Load a configuration file from a path
   * eg: file://.../config/application.conf
   *
   * @param path the path where the config file is located.
   * @return the configuration loaded as [[Configuration]]
   */
  def loadFromPath(path: String): Configuration = {
    ConfigSource
      .file(path)
      .load[Configuration]
  }

  /**
   * Load a configuration file from a given string
   *
   * @param configString the configuration as a string.
   * @return the configuration loaded as [[Configuration]]
   */
  def loadFromString(configString: String): Configuration = {
    ConfigSource
      .string(configString)
      .load[Configuration]
  }
}
