package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.{Configuration, RuntimeETLContext}
import bio.ferlab.datalake.spark3.utils.DeltaUtils.{compact, vacuum}
import mainargs.{ParserForMethods, arg, main}
import org.apache.spark.sql.SparkSession

case class OptimizeDeltaTables(rc: RuntimeETLContext, datasetIds: Seq[String], numberOfVersions: Int) {
  implicit val conf: Configuration = rc.config
  implicit val spark: SparkSession = rc.spark

  def run(): Unit = {
    datasetIds.foreach { id =>
      val ds = conf.getDataset(id)
      compact(ds)
      vacuum(ds, numberOfVersions)
    }
  }
}

object OptimizeDeltaTables {
  @main
  def run(rc: RuntimeETLContext,
          @arg(name = "dataset_ids", short = 'd', doc = "Dataset Ids") datasetIds: Seq[String],
          @arg(name = "number_of_versions", short = 'n', doc = "Number of Versions") numberOfVersions: Int): Unit = {
    OptimizeDeltaTables(rc, datasetIds, numberOfVersions).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}


