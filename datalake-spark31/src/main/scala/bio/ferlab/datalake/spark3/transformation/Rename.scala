package bio.ferlab.datalake.spark3.transformation

import org.apache.spark.sql.DataFrame

case class Rename(renameMap: Map[String, String]) extends Transformation {
  override def transform: DataFrame => DataFrame = { df =>
    renameMap.foldLeft(df) {
      case (d, (existingName, newName)) => d.withColumnRenamed(existingName, newName)
    }
  }
}

