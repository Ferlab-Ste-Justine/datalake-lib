package bio.ferlab.datalake.commons.config

abstract class BaseDatasets {

  def tableDatabase: Option[String]

  def viewDatabase: Option[String]

  def sources: List[DatasetConf]

  def table(tableName: String): Option[TableConf] = tableDatabase.map(t => TableConf(t, tableName))

  def view(viewName: String): Option[TableConf] = viewDatabase.map(v => TableConf(v, viewName))
}
