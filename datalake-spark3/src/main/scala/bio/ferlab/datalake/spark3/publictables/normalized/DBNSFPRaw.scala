package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class DBNSFPRaw()(implicit conf: Configuration) extends ETLSingleDestination{
  override val mainDestination: DatasetConf = conf.getDataset("normalized_dbnsfp")
  val raw_dbnsfp: DatasetConf = conf.getDataset("raw_dbnsfp")
  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(raw_dbnsfp.id -> raw_dbnsfp.read)
  }

  override def transformSingle(data: Map[String, DataFrame],
                         lastRunDateTime: LocalDateTime = minDateTime,
                         currentRunDateTime: LocalDateTime = LocalDateTime.now())(implicit spark: SparkSession): DataFrame = {
    data(raw_dbnsfp.id)
      .withColumnRenamed("#chr", "chr")
      .withColumnRenamed("position_1-based", "start")
  }


}
