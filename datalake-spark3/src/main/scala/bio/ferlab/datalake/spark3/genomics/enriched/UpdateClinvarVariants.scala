package bio.ferlab.datalake.spark3.genomics.enriched

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.genomics.enriched.Variants.DataFrameOps
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

/**
 * This ETL update column clinvar in variants table with latest version of clinvar table
 *
 * @param rc                the etl context
 */
case class UpdateClinvarVariants(rc: RuntimeETLContext) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_variants")
  protected val clinvar: DatasetConf = conf.getDataset("normalized_clinvar")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(
      clinvar.id -> clinvar.read,
      mainDestination.id -> mainDestination.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(mainDestination.id).drop("clinvar").withClinvar(data(clinvar.id))
  }

}

