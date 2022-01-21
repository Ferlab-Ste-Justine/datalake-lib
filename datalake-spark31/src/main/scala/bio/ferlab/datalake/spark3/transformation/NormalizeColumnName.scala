package bio.ferlab.datalake.spark3.transformation

import bio.ferlab.datalake.spark3.transformation.NormalizeColumnName.{replace_special_char_by_ansii_code, replace_special_char_by_underscore}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

case class NormalizeColumnName(columns: String*) extends Transformation {

  /**
   * Apply a normalization over a dataframe column names.
   * First try to replace all special char by underscores.
   * If it doesn't create duplicates then apply this normalization
   * Else, replace all special char by the ansii code
   * Finally, if the latter normalization creates duplicates, add the idx of the column to the name.
   *
   * @param df dataframe to normalize
   * @param columns columns to normalize
   * @return a normalized dataframe
   */
  def normalizeColumnName(df: DataFrame, columns: List[String]): DataFrame = {
    val columnsNormalized = columns.map(replace_special_char_by_underscore)
    if (columnsNormalized.distinct.length == columns.length) {
      columns.foldLeft(df)((d, c) => d.withColumnRenamed(c, replace_special_char_by_underscore(c)))
    } else {
      val renamePair: List[Column] =
        columns
          .map { c => c -> replace_special_char_by_ansii_code(c, df.columns.indexOf(c), df.columns)}
          .map { case (oldName, newName) => col(oldName) as newName}

      df.select(renamePair:_*)
    }
  }

  /**
   * Main method of the trait.
   * It defines the logic to transform the input dataframe.
   *
   * @return a transformed dataframe
   */
  override def transform: DataFrame => DataFrame = {df =>
    columns match {
      case Nil => normalizeColumnName(df, df.columns.toList)
      case _ => normalizeColumnName(df, columns.toList)
    }
  }
}

object NormalizeColumnName {
  val replace_special_char_by_ansii_code: (String, Int, Array[String]) => String = {
    case (column_s, idx_col, columns_a) =>
      val idx_pos = "[^a-zA-Z0-9_]".r.findAllMatchIn(column_s).map(_.start).toList
      if(idx_pos.nonEmpty) {
        var column_vs = column_s
        var idx_adj = 0
        for (i <- idx_pos) {
          val j = i + idx_adj
          val subs = "_" + column_vs(j).toInt
          column_vs = column_vs.patch(j, subs, 1)
          idx_adj = idx_adj + subs.length - 1
        }
        // If modification results in a value already in the list ex A B (changes to A_32B) and A_32B add column for uniqueness
        if(columns_a.contains(column_vs)){
          column_vs = column_vs + "_" + idx_col
        }
        column_vs
      } else {
        column_s
      }
  }

  val replace_special_char_by_underscore: String => String = _.replaceAll("[^a-zA-Z0-9_]", "_").replaceAll("_{2,}", "_")
}