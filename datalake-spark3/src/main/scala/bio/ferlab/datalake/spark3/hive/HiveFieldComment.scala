package bio.ferlab.datalake.spark3.hive

case class HiveFieldComment(col_name: String,
                            data_type: String,
                            comment: String)
