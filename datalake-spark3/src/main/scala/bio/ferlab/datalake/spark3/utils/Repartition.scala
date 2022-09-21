package bio.ferlab.datalake.spark3.utils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

trait Repartition extends Function[DataFrame, DataFrame] {
  def repartition(df: DataFrame): DataFrame

  protected def sortWithinPartition(unsortedDF: DataFrame, sortColumns: Seq[Column]): DataFrame = sortColumns match {
    case Nil => unsortedDF
    case _ => unsortedDF.sortWithinPartitions(sortColumns: _*)
  }

  override def apply(df: DataFrame): DataFrame = repartition(df)
}

case object IdentityRepartition extends Repartition {
  override def repartition(df: DataFrame): DataFrame = df
}

case class FixedRepartition(n: Int, sortColumns: Seq[Column] = Nil) extends Repartition {
  override def repartition(df: DataFrame): DataFrame = sortWithinPartition(df.repartition(n), sortColumns)
}

case class RepartitionByColumns(columnNames: Seq[String], n: Option[Int] = None, sortColumns: Seq[Column] = Nil) extends Repartition {
  override def repartition(df: DataFrame): DataFrame = {
    val unsortedDF = n match {
      case Some(i) => df.repartition(i, columnNames.map(col): _*)
      case _ => df.repartition(columnNames.map(col): _*)
    }
    sortWithinPartition(unsortedDF, sortColumns)
  }
}

case class RepartitionByRange(columnNames: Seq[String], n: Option[Int] = None, sortColumns: Seq[Column] = Nil) extends Repartition {
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

case class Coalesce(n: Int = 1) extends Repartition{
  override def repartition(df: DataFrame): DataFrame = df.coalesce(n)
}