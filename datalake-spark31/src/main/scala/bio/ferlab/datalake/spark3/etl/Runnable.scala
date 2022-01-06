package bio.ferlab.datalake.spark3.etl

import bio.ferlab.datalake.commons.config.RunStep
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

trait Runnable {

  /**
   * Entry point of the etl - execute this method in order to run the whole ETL
   * @param spark an instance of SparkSession
   */
  def run(runSteps: Seq[RunStep] = RunStep.default_load,
          lastRunDateTime: Option[LocalDateTime] = None,
          currentRunDateTime: Option[LocalDateTime] = None)(implicit spark: SparkSession): Map[String, DataFrame]

}
