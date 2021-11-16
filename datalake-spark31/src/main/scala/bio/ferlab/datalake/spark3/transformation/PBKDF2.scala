package bio.ferlab.datalake.spark3.transformation

import com.roundeights.hasher.Implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.language.postfixOps

/**
 * example of usage: PBKDF2("salt", 100000, 512, "NoDossierClient", "NoAssuranceMaladie", ...))
 *
 * @param salt
 * @param iteration
 * @param keyLength
 * @param columns
 */
case class PBKDF2(salt: String, iteration: Int, keyLength: Int, columns: String*) extends Transformation {

  /**
   *
   * @param id
   * @param salt
   * @return
   */
  def pbkdf2(id: String, salt: String): String = {
    id.pbkdf2(salt, iteration, keyLength).hex
  }

  val pbkdf2Udf = udf((id: String, salt: String) => pbkdf2(id, salt))

  override def transform: DataFrame => DataFrame = { df =>

    val spark = df.sparkSession
    spark.udf.register("pbkdf2Udf", pbkdf2Udf)

    columns.foldLeft(df) { case (d, column) =>
      d.withColumn(column,
        when(col(column).isNull, lit(null).cast(StringType)).otherwise(callUDF("pbkdf2Udf", col(column), lit(salt))))

    }

  }

}