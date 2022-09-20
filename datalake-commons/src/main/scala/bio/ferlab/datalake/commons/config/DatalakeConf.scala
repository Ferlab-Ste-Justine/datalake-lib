package bio.ferlab.datalake.commons.config

/**
 * Base configuration needed for an ETL job
 * @param storages list of storages associated with aliases
 * @param sources list of data sources
 * @param args arguments passed to the job
 * @param sparkconf extra configuration for the spark conf
 */
case class DatalakeConf(storages: List[StorageConf] = List(),
                        sources: List[DatasetConf] = List(),
                        args: List[String] = List.empty[String],
                        sparkconf: Map[String, String] = Map()) extends Configuration {

  def +(that: DatalakeConf): DatalakeConf =
    DatalakeConf(
      this.storages ++ that.storages,
      this.sources ++ that.sources,
      this.args ++ that.args,
      this.sparkconf ++ that.sparkconf,
    )
}

object DatalakeConf {
  def empty: DatalakeConf = DatalakeConf()
}
