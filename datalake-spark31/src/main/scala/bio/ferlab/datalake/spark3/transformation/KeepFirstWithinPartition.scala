package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.{Column, DataFrame}

import java.util.UUID

case class KeepFirstWithinPartition(partitionByExpr: Seq[String],
                                    orderByExpr: Column*) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    val window = Window.partitionBy(partitionByExpr.map(col):_*).orderBy(orderByExpr:_*)

    val randomName = UUID.randomUUID().toString
    df
      .withColumn(randomName, row_number.over(window))
      .where(col(randomName) === lit(1))
      .drop(randomName)

  }
}

