package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql._

import scala.language.postfixOps
import scala.util.matching.Regex


case class CamelToSnake(columns: String*) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>

    columns.foldLeft(df) { case (d, column) =>
      d.withColumnRenamed(column, CamelToSnake.camel2Snake(column))
    }
  }
}

object CamelToSnake {
  val separatees: Regex = "[a-z](?=[A-Z])|[0-9](?=[a-zA-Z])|[A-Z](?=[A-Z][a-z])|[a-zA-Z](?=[0-9]|[-~!@#$^%&*()+={}\\\\[\\\\]|;:\\\"'`<,>.?/\\\\\\\\])".r

  def camel2Snake(s: String): String = separatees.replaceAllIn(s, _.group(0) + '_').toLowerCase
}