package bio.ferlab.datalake.commons.config

/**
 * Configuration for a table
 *
 * @param database name of the database / schema
 * @param name     name of the table / view
 */
case class TableConf(database: String,
                     name: String) {
  def fullName: String = {
    if (database == "") {
      name
    } else {
      s"${database}.${name}"
    }
  }
}
