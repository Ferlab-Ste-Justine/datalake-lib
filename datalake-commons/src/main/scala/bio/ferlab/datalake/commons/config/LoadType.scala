package bio.ferlab.datalake.commons.config

import enum.Enum

/**
 * Defines the different ways of persisting data into a dataset.
 */
sealed trait LoadType

/**
 * List of all LoadTypes supported.
 */
object LoadType {
  /**
   * When the dataset is cannot be written into.
   */
  case object Read extends LoadType

  /**
   * Does not change the data, regroups several small files into bigger files.
   * Pros:
   *  - Improve reading performance if the resulting files are properly sized.
   *  - Improve merge/update performance.
   * Cons:
   *  - Takes time
   */
  case object Compact extends LoadType

  /**
   * Create or replace existing data present in the dataset.
   * Pros:
   *  - Very fast writing performance.
   *  - Very fast reading performance.
   * Cons:
   *  - Does not take into account previous loads.
   */
  case object OverWrite extends LoadType

  /**
   * Create or replace existing partitions in the dataset.
   * Pros:
   *  - If the dataset is partitioned by a logical entity like a study_id, a batch_id or a date, it allows to recompute only certain partitions.
   *  - Can be useful when the partitions are very big
   *  - Another worse solution is to split the partitions into several table. This is worse it's harder to enforce a consistent schema across multiple tables.
   *    And also, it's easier for the end-user to retrieve the data as it's located at only one place instead of a list of locations.
   * Cons:
   *  - Previous partition with the same name will be removed and can produce loss of data if used incorrectly.
   */
  case object OverWritePartition extends LoadType

  /**
   * Insert the new data into the dataset without taking into account previous loads.
   * Pros:
   *  - Very fast writing
   *  - Fast reading when the files are sized correctly
   * Cons:
   *  - Can produce duplicates and inconsistency if the same rows are persisted multiple times.
   *  - Will produce an large number of files over time. This usually is combined with 'Compact' in order to mitigate the problem.
   */
  case object Insert extends LoadType

  /**
   * Update or Insert the data based on a set of column defining the primary key.
   * Pros:
   *  - Allows for duplicates and update management compared to Insert.
   *  - Faster write performance than a Scd2
   *  Cons:
   *  - A bit more costly than Overwrite, OverWritePartition and Insert.
   *  - Does not keep an historic.
   *  - Depending on implementation, can fail if the new data contains multiple rows with the same primary keys.
   *  - Depending on implementation, the number of small files can increase over time.
   */
  case object Upsert extends LoadType

  /**
   * Update or Insert the data based on a set of column defining the primary key.
   * And also maintain two columns 'created_on' and 'updated_on'.
   * Pros:
   *  - Allows for duplicates and update management compared to Insert.
   *  - Faster write performance than a Scd2
   *  Cons:
   *  - A bit more costly than Overwrite, OverWritePartition and Insert.
   *  - Does not keep an historic.
   *  - Depending on implementation, can fail if the new data contains multiple rows with the same primary keys.
   *  - Depending on implementation, the number of small files can increase over time.
   */
  case object Scd1 extends LoadType



  /**
   * Update or Insert the data based on a set of column defining the primary key.
   * And also maintain an historic of how the data changed over time by adding a new row each time
   * a row is updated.
   * Pros:
   *  - Allows for duplicates and update management compared to Insert.
   *  - Keeps an historic of all the changes
   *  Cons:
   *  - Costly and increasingly costlier overtime.
   *  - Depending on implementation, can fail if the new data contains multiple rows with the same primary keys.
   *  - Depending on implementation, the number of small files can increase over time.
   */
  case object Scd2 extends LoadType

  implicit val EnumInstance: Enum[LoadType] = Enum.derived[LoadType]
}
