package bio.ferlab.datalake.commons.config

import pureconfig.ConfigReader


case class SimpleConfiguration(datalake: DatalakeConf) extends ConfigurationWrapper(datalake) {

}

object SimpleConfiguration {
  import pureconfig.generic.auto._
  import pureconfig.generic.semiauto._

  implicit val configReader: ConfigReader[SimpleConfiguration] = deriveReader[SimpleConfiguration]

}
