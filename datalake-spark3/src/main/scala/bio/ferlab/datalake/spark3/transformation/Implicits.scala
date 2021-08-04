package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.{Column, DataFrame}

object Implicits {
  implicit class DataFrameOperations(df: DataFrame) {

    def keepFirstWithinPartition(partitionByExpr: Seq[String],
                                 orderByExpr: Column*): DataFrame =
      KeepFirstWithinPartition(partitionByExpr, orderByExpr:_*).transform(df)
  }

}
