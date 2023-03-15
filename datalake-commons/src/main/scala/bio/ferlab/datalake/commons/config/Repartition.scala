package bio.ferlab.datalake.commons.config

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import pureconfig.ConfigFieldMapping
import pureconfig.generic.FieldCoproductHint

sealed trait Repartition extends Function[DataFrame, DataFrame] {

  def repartition(df: DataFrame): DataFrame

  protected def sortWithinPartition(unsortedDF: DataFrame, sortColumns: Seq[String]): DataFrame = sortColumns match {
    case Nil => unsortedDF
    case _ => unsortedDF.sortWithinPartitions(sortColumns.map(col): _*)
  }

  override def apply(df: DataFrame): DataFrame = repartition(df)
}

object Repartition {
  implicit val hint: FieldCoproductHint[Repartition] = new FieldCoproductHint[Repartition]("kind") {
    override def fieldValue(name: String): String = fieldMapping(name)
  }
}

case object IdentityRepartition extends Repartition {
  override def repartition(df: DataFrame): DataFrame = df
}

case class FixedRepartition(n: Int, sortColumns: Seq[String] = Nil) extends Repartition {
  override def repartition(df: DataFrame): DataFrame = sortWithinPartition(df.repartition(n), sortColumns)
}

case class RepartitionByColumns(columnNames: Seq[String], n: Option[Int] = None, sortColumns: Seq[String] = Nil) extends Repartition {
  override def repartition(df: DataFrame): DataFrame = {
    val unsortedDF = n match {
      case Some(i) => df.repartition(i, columnNames.map(col): _*)
      case _ => df.repartition(columnNames.map(col): _*)
    }
    sortWithinPartition(unsortedDF, sortColumns)
  }
}

case class RepartitionByRange(columnNames: Seq[String], n: Option[Int] = None, sortColumns: Seq[String] = Nil) extends Repartition {
  override def repartition(df: DataFrame): DataFrame = {
    val unsortedDF = n match {
      case Some(i) => df.repartitionByRange(i, columnNames.map(col): _*)
      case _ => df.repartitionByRange(columnNames.map(col): _*)
    }
    sortWithinPartition(unsortedDF, sortColumns)
  }
}

case class DynamicRepartition(n: Int = 2000000) extends Repartition {
  override def repartition(df: DataFrame): DataFrame = {
    val persisted = df.persist()
    val rowCount: Long = persisted.count()
    persisted.repartition((rowCount.toDouble / n.toDouble).ceil.toInt)
  }
}

case class Coalesce(n: Int = 1) extends Repartition {
  override def repartition(df: DataFrame): DataFrame = df.coalesce(n)
}