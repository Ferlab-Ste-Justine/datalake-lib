package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.commons.config.SimpleConfiguration
import pureconfig.generic.auto._

abstract class SparkApp extends SparkAppWithConfig[SimpleConfiguration] {

}
