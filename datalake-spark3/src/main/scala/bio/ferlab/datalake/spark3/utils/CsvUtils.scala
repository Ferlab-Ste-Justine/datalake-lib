package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.Format.CSV
import bio.ferlab.datalake.commons.config.{Coalesce, Configuration, DatasetConf, FixedRepartition}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvUtils {

  /**
   * Renames the CSV output file if the destination format is CSV and data is repartitioned into a single file.
   *
   * When writing to CSV format, Spark adds the partition information to the filename. This function replaces the
   * partition info with the table name. It also deletes the unnecessary `_SUCCESS` file created by Spark.
   *
   * @param mainDestination The mainDestination [[DatasetConf]] of the ETL
   * @param suffix          Optional, adds a suffix to the file name, before the extension
   * @param spark           An instance of [[SparkSession]]
   * @param conf            The ETL [[Configuration]]
   * @return The renamed CSV loaded as a Dataframe
   * @example
   * This function would rename this CSV file :
   * {{{
   *   published/nom_du_projet/nom_de_la_table/part-00000-3afd3298-a186-4289-8ba3-3bf55d27953f-c000.csv
   * }}}
   * to :
   * {{{
   *   published/nom_du_projet/nom_de_la_table/nom_de_la_table_suffix.csv
   * }}}
   * where suffix could be : `v1_0_0`, `2020_01_01`, etc.
   */
  def renameCsvFile(mainDestination: DatasetConf, suffix: Option[String] = None)
                   (implicit spark: SparkSession, conf: Configuration): DataFrame = {
    val (format, repartition) = (mainDestination.format, mainDestination.repartition)
    if (format == CSV && repartition.isDefined) {
      if (repartition.get == FixedRepartition(1) || repartition.get == Coalesce(1)) {
        val fs = FileSystemResolver.resolve(conf.getStorage(mainDestination.storageid).filesystem)
        val files = fs.list(mainDestination.location, recursive = false)
        val successFilePath = files
          .filter(_.name == "_SUCCESS")
          .head
          .path
        val csvFilePath = files
          .filter(_.name.startsWith("part-"))
          .head
          .path

        val newPath = mainDestination.location + "/" + mainDestination.path.split("/").last + suffix.map("_" + _).getOrElse("") + ".csv"

        fs.move(csvFilePath, newPath, overwrite = true)
        fs.remove(successFilePath)
      }
    }
    mainDestination.read
  }

}
