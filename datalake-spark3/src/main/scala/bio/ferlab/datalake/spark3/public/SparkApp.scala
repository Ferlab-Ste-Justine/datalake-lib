package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.commons.config.SimpleConfiguration

import pureconfig.generic.auto._
import pureconfig.module.enum._

abstract class SparkApp extends SparkAppWithConfig[SimpleConfiguration] {

}
