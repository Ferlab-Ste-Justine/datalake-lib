package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.SimpleConfiguration

import java.time.LocalDateTime


package object v4 {
  // Defaults to timestamp-based ETLs
  type SimpleETL = ETL[LocalDateTime, SimpleConfiguration]
  type SimpleETLP = ETLP[LocalDateTime, SimpleConfiguration]
  type SimpleSingleETL = SingleETL[LocalDateTime, SimpleConfiguration]
  type SimpleTransformationsETL = TransformationsETL[LocalDateTime, SimpleConfiguration]

  // Timestamp-based ETLs
  type SimpleTimestampETL = ETL[LocalDateTime, SimpleConfiguration]
  type SimpleTimestampETLP = ETLP[LocalDateTime, SimpleConfiguration]
  type SimpleSingleTimestampETL = SingleETL[LocalDateTime, SimpleConfiguration]
  type SimpleTransformationsTimestampETL = TransformationsETL[LocalDateTime, SimpleConfiguration]

  // Id-based ETLs
  type SimpleIdETL = ETL[String, SimpleConfiguration]
  type SimpleIdETLP = ETLP[String, SimpleConfiguration]
  type SimpleSingleIdETL = SingleETL[String, SimpleConfiguration]
  type SimpleTransformationsIdETL = TransformationsETL[String, SimpleConfiguration]
}
