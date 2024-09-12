package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.utils.DeltaUtils.{compact, vacuum}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

trait DeltaTableOptimization {
  def optimize(ds: DatasetConf, numberOfVersions: Int)(implicit conf: Configuration, spark: SparkSession): Unit = {
    val df = ds.read

    // compact latest partition. If no partition specified, compact all.
    if (ds.partitionby.isEmpty) {
      compact(ds)
    } else if (ds.partitionby.length == 1) { // create partition filter based on single partition column
      val maxPartition = df.select(max(col(ds.partitionby.head))).collect().head.get(0)
      val partitionFilter = s"${ds.partitionby}='${maxPartition.toString}'"
      compact(ds, Some(partitionFilter))
    } else { // create partition filter based on all partition columns
      val maxPartitions = ds.partitionby.map(p => {
        df.select(max(col(p))).collect().head.get(0)
      }).zip(ds.partitionby)
      val partitionFilters = maxPartitions.map(m => s"${m._2}='${m._1.toString}'")
      val finalPartitionFilter = partitionFilters.mkString(" AND ")
      compact(ds, Some(finalPartitionFilter))
    }

    vacuum(ds, numberOfVersions)
  }
}
