package bio.ferlab.datalake.spark3.etl

import org.apache.spark.sql.DataFrame

@Deprecated
case class Partitioning(repartitionExpr: DataFrame => DataFrame,
                        partitionBy: Seq[String]) {

}

@Deprecated
object Partitioning {
  def default: Partitioning = Partitioning({ df => df }, Seq())
}
