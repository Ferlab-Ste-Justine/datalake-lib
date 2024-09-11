package bio.ferlab.datalake.spark3.etl.v4

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.DeltaUtils.{compact, vacuum}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

trait DeltaTableOptimization {
  def optimize(ds: DatasetConf, numberOfVersions: Int)(implicit conf: Configuration, spark: SparkSession): Unit = {
    val df = ds.read

    if(ds.partitionby.isEmpty){
      compact(ds)
    } else { // elif length is 1
      val maxPartition = df.select(max(col(ds.partitionby.head))).collect().head.get(0)
      val partitionFilter = s"${ds.partitionby}='${maxPartition.toString}'"
      compact(ds, Some(partitionFilter))
    } // else filter on all of them, get max of each, do not read entire table

    vacuum(ds, numberOfVersions)
  }
}
