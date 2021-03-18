package bio.ferlab.datalake.core.etl

import org.apache.spark.sql.Column

case class Partitioning(repartitionExpr: Seq[Column],
                        sortWithinPartitions: Seq[Column],
                        partitionBy: Seq[String]) {

}

object Partitioning {
  def default: Partitioning = Partitioning(Seq(), Seq(), Seq())
}
