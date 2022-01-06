package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.{Column, DataFrame}

import java.util.UUID

case class DropDuplicates(subset: Seq[String],
                          orderBy: Column*) extends Transformation {
  override def transform: DataFrame => DataFrame = {df =>
    if (subset.isEmpty) {
      df.dropDuplicates()
    } else {
      val window = Window.partitionBy(subset.map(col):_*).orderBy(orderBy:_*)

      val randomName = UUID.randomUUID().toString
      df
        .withColumn(randomName, row_number.over(window))
        .where(col(randomName) === lit(1))
        .drop(randomName)
    }
  }
}

object DropDuplicates {
  def apply(): DropDuplicates = new DropDuplicates(Seq())
}

