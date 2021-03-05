package bio.ferlab.datalake.core.config

import pureconfig._
import pureconfig.generic.auto._

object ConfigurationLoader {

  def loadEtlConfiguration(configFile: String): Configuration = {

    ConfigSource
      .file(configFile)
      .load[Configuration]
      .fold(
        _ => ConfigSource.default.load[Configuration],
        conf => Right(conf))
      .fold(failure => throw new IllegalArgumentException(failure.prettyPrint()), conf => conf)

  }
}
