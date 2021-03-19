package bio.ferlab.datalake.core.etl

import org.apache.spark.sql.DataFrame

case class Partitioning(repartitionExpr: DataFrame => DataFrame,
                        partitionBy: Seq[String]) {

}

object Partitioning {
  def default: Partitioning = Partitioning({ df => df }, Seq())
}
