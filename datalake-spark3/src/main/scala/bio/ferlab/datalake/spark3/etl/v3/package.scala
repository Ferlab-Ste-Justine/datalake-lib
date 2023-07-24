package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.SimpleConfiguration

package object v3 {
  type SimpleETL = ETL[SimpleConfiguration]
  type SimpleETLP = ETLP[SimpleConfiguration]
  type SimpleSingleETL = SingleETL[SimpleConfiguration]
  type SimpleTransformationsETL = TransformationsETL[SimpleConfiguration]
}
