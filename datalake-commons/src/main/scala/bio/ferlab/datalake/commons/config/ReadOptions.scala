package bio.ferlab.datalake.commons.config

/**
 * List of read options supported. More options are supported.
 */
object ReadOptions {
  final val PARTITION_COLUMN = "partitionColumn"
  final val LOWER_BOUND = "lowerBound"
  final val UPPER_BOUND = "upperBound"
  final val NUM_PARTITIONS = "numPartitions"
}
