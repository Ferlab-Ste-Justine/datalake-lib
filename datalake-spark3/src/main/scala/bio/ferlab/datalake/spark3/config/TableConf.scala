package bio.ferlab.datalake.spark3.config

/**
 * Configuration for a table
 * @param database name of the database / schema
 * @param name name of the table / view
 */
case class TableConf(database: String,
                     name: String) {
  def fullName: String = s"${database}.${name}"
}
