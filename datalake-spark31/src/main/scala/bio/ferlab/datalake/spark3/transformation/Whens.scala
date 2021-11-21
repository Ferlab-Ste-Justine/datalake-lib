package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

case class Whens(column: String, conditions: List[(Column, Any)], otherwise: Any) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>

    //step 1 initial
    val initialWhen = when(conditions.head._1, conditions.head._2)

    //step 2 add all the when() together
    val whens: Column =
      conditions.tail.foldLeft(initialWhen) {
        case (previousWhen, (currentCondition, currentValue)) =>
          previousWhen.when(currentCondition, currentValue)
      }

    //step 3 add otherwise() to all the whens()
    val whensWithOtherwise = whens.otherwise(otherwise)

      //step 4 create a column with the when statement
    df.withColumn(column, whensWithOtherwise)
  }
}








