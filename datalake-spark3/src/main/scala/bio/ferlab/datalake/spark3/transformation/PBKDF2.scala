package bio.ferlab.datalake.spark3.transformation

import bio.ferlab.datalake.spark3.transformation.PBKDF2.pbkdf2Udf
import com.roundeights.hasher.Implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.language.postfixOps

/**
 * example of usage: PBKDF2("salt", 100000, 512, "NoDossierClient", "NoAssuranceMaladie", ...))
 *
 * @param salt random salt string
 * @param iteration number of iteration
 * @param keyLength length of the resulting hash
 * @param columns names of the columns to hash
 */
case class PBKDF2(salt: String, iteration: Int, keyLength: Int, override val columns: String*) extends HashTransformation[Seq[String]] {

  override def transform: DataFrame => DataFrame = { df =>

    val spark = df.sparkSession
    spark.udf.register("pbkdf2Udf", pbkdf2Udf)

    columns.foldLeft(df) { case (d, column) =>
      d.withColumn(column,
        when(col(column).isNull, nullValues).otherwise(callUDF("pbkdf2Udf", col(column), lit(salt), lit(iteration), lit(keyLength))))

    }
  }

}

object PBKDF2 {

  /**
   *
   * @param id string to be hashed
   * @param salt random salt string
   * @return hashed and salted string
   */
  def pbkdf2(id: String, salt: String, iteration: Int, keyLength: Int): String = {
    id.pbkdf2(salt, iteration, keyLength).hex
  }

  val pbkdf2Udf: UserDefinedFunction = udf((id: String, salt: String, iteration: Int, keyLength: Int) => pbkdf2(id, salt, iteration, keyLength))
}