package bio.ferlab.datalake.spark3.testmodels.normalized

case class NormalizedDbsnp(`chromosome`: String = "1",
                           `start`: Long = 69897,
                           `end`: Long = 69898,
                           `reference`: String = "T",
                           `alternate`: String = "C",
                           `name`: String = "rs200676709",
                           `original_contig_name`: String = "NC_000001.11")
