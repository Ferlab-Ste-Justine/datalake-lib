package bio.ferlab.datalake.core.loader

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

trait Loader {

  /**
   * Overwrites the data located in output/tableName
   * usually used for small/test tables.
   * @param df the data to write
   * @param tableName the name of the table
   * @param location full path of where the data will be located
   * @param repartitionExpr OPTIONAL - repartition logic
   * @param sortWithinPartitions OPTIONAL - sort within partition logic
   * @param partitionBy OPTIONAL - partition configuration
   * @param dataChange if the data is expected to be different from the data already written
   * @param spark a valid spark session
   * @return the data as a dataframe
   */
  def writeOnce(location: String,
                tableName: String,
                df: DataFrame,
                repartitionExpr: Seq[Column],
                sortWithinPartitions: Seq[Column],
                partitionBy: Seq[String],
                dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame

  /**
   * Update or insert data into a table
   * usually used for fact table where duplicates need to be resolved as one record.
   * @param location full path of where the data will be located
   * @param tableName the name of the updated/created table
   * @param updates new data to be merged with existing data
   * @param uidName name of the column holding the unique id
   * @param repartitionExpr OPTIONAL - repartition logic
   * @param sortWithinPartitions OPTIONAL - sort within partition logic
   * @param partitionBy OPTIONAL - partition configuration
   * @param dataChange if the data is expected to be different from the data already written
   * @param spark a valid spark session
   * @return the data as a dataframe
   */
  def upsert(location: String,
             tableName: String,
             updates: DataFrame,
             uidName: String,
             repartitionExpr: Seq[Column],
             sortWithinPartitions: Seq[Column],
             partitionBy: Seq[String],
             dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame

  /**
   * Update the data only if the data has changed
   * Insert new data
   * maintains updatedOn and createdOn timestamps for each record
   * usually used for dimension table where keeping the full historic of the data is not important.
   * @param location full path of where the data will be located
   * @param tableName the name of the updated/created table
   * @param updates new data to be merged with existing data
   * @param uidName name of the column holding the unique id
   * @param oidName name of the column holding the hash of the column that can change over time (or version number)
   * @param createdOnName name of the column holding the creation timestamp
   * @param updatedOnName name of the column holding the last update timestamp
   * @param repartitionExpr OPTIONAL - repartition logic
   * @param sortWithinPartitions OPTIONAL - sort within partition logic
   * @param partitionBy OPTIONAL - partition configuration
   * @param dataChange if the data is expected to be different from the data already written
   * @param spark a valid spark session
   * @return the data as a dataframe
   */
  def scd1(location: String,
           tableName: String,
           updates: DataFrame,
           uidName: String,
           oidName: String,
           createdOnName: String,
           updatedOnName: String,
           repartitionExpr: Seq[Column],
           sortWithinPartitions: Seq[Column],
           partitionBy: Seq[String],
           dataChange: Boolean = true)(implicit spark: SparkSession): DataFrame

}
