package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql._
import scala.language.postfixOps


case class CamelToSnake(columns: String*) extends Transformation {

  val separatees = "[a-z](?=[A-Z])|[0-9](?=[a-zA-Z])|[A-Z](?=[A-Z][a-z])|[a-zA-Z](?=[0-9]|[-~!@#$^%&*()+={}\\\\[\\\\]|;:\\\"'`<,>.?/\\\\\\\\])".r

  def camel2Snake(s: String): String = separatees.replaceAllIn(s, _.group(0) + '_').toLowerCase

  override def transform: DataFrame => DataFrame = { df =>

    columns.foldLeft(df) { case (d, column) =>
      d.withColumnRenamed(column, camel2Snake(column))
    }
  }
}