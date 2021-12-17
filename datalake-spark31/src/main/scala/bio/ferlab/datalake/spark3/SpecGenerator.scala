package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.spark3.transformation.CamelToSnake
import bio.ferlab.datalake.spark3.utils.ClassGenerator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object SpecGenerator {

  def printSpec(df: DataFrame,
                schema: String,
                tableName: String,
                bucket: String = "s3a://red-prd/raw",
                format: String = "DELTA",
                loadType: String = "INSERT")
               (implicit spark: SparkSession): Unit = {
    val lcSchema = schema.toLowerCase
    val lcTableName = tableName.toLowerCase
    println(s"${lcSchema}_$lcTableName,${loadType},${bucket}/$lcSchema/$lcTableName,$format")
    df.schema.fields.foreach{
      case StructField(name, dataType, _, _) =>
        val camelName = CamelToSnake("").camel2Snake(name)
        val spec = s"$name,${ClassGenerator.getType(dataType)},-,$camelName,${ClassGenerator.getType(dataType)}"
        if(camelName!=name) println(s"$spec,CamelToSnake")
        else println(spec)
    }
  }

}
