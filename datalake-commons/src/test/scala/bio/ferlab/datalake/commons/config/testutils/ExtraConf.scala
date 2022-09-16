package bio.ferlab.datalake.commons.config.testutils

import bio.ferlab.datalake.commons.config.{ConfigurationWrapper, DatalakeConf}

case class ExtraConf(extraOption: String, datalake: DatalakeConf) extends ConfigurationWrapper(datalake)

