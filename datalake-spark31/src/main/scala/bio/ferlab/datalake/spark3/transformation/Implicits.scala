package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.{Column, DataFrame}

object Implicits {
  implicit class DataFrameOperations(df: DataFrame) {

    def dropDuplicates(partitionByExpr: Seq[String],
                       orderByExpr: Column*): DataFrame =
      DropDuplicates(partitionByExpr, orderByExpr:_*).transform(df)

    @deprecated("use [[dropDuplicates]]", "0.2.3")
    def keepFirstWithinPartition(partitionByExpr: Seq[String],
                                 orderByExpr: Column*): DataFrame =
      KeepFirstWithinPartition(partitionByExpr, orderByExpr:_*).transform(df)
  }

}
