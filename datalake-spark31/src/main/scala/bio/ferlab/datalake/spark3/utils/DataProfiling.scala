package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.spark3.transformation.CamelToSnake.camel2Snake
import bio.ferlab.datalake.spark3.transformation.NormalizeColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.slf4j

object DataProfiling {

  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def showSchema(schema: String, numRows: Int = 50)
                (implicit spark: SparkSession, dfr: DataFrameReader): DataFrame = {

    val query = s"SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema='$schema'"

    val df = dfr.option("query", query)
      .load()
      .orderBy("table_name")
    df.show(numRows, false)
    df
  }

  def showSchemas(numRows: Int = 50)(implicit spark: SparkSession, dfr: DataFrameReader): DataFrame = {

    val query = s"SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables"

    val df = dfr.option("query", query)
      .load()
      .groupBy(col("table_schema"))
      .count()
      .orderBy("table_schema")
    df.show(numRows, false)
    df
  }

  def sql(query: String, numRows: Int = 50)
         (implicit spark: SparkSession, dfr: DataFrameReader): DataFrame = {

    val df = dfr.option("query", query)
      .load()
    df.show(numRows, false)
    df
  }

  def printIngestionSpec(df: DataFrame,
                         schema: String,
                         tableName: String,
                         systemName: String,
                         folder: String,
                         format: String,
                         loadType: String): Unit = {

    val lcSchema = schema.toLowerCase
    println(s"$schema.$tableName,${format},${folder}/$systemName/${camel2Snake(tableName)},${lcSchema}_${camel2Snake(tableName)},$loadType")

    val newNames = NormalizeColumnName.normalizeColumnName(df, df.columns.toList).columns

    df.schema.fields.zip(newNames).foreach{
      case (StructField(name, dataType, _, _), normalizedName) =>
        val spec = s"$name,${ClassGenerator.getType(dataType)},-,$normalizedName,${ClassGenerator.getType(dataType)}"
        println(s"$spec")
    }
  }

  def externalDsId(schema: String, tableName: String, idPadTo: Int = 55): String = {
    s""""${schema}.${tableName.replaceAll(" ", "_")}"""".padTo(idPadTo, " ").mkString
  }

  def rawDsId(schema: String, tableName: String, idPadTo: Int = 55): String = {
    s""""raw_${schema.toLowerCase}_${tableName.toLowerCase.replaceAll(" ", "_")}"""".padTo(idPadTo, " ").mkString
  }

  def printSourceDatasets(schema: String,
                          tables: List[String],
                          format: String,
                          storageId: String): Unit = {

    val idPadTo = tables.map(t => externalDsId(schema, t, 0).length).max + 1
    val tablePadTo = tables.map(_.length).max + 1
    tables.foreach(table =>
      printSourceDataset(schema, table, format, storageId, idPadTo, tablePadTo)
    )
  }

  def printSourceDataset(schema: String,
                         tableName: String,
                         format: String,
                         storageId: String,
                         idPadTo: Int = 55,
                         tablePadTo: Int = 35): Unit = {

    val id = externalDsId(schema, tableName, idPadTo)
    val table = s""""${tableName}"""".padTo(tablePadTo, " ").mkString
    println(s"""DatasetConf(${id}, $storageId,"", $format, Read, Some(TableConf("${schema}", $table)), readoptions = ${storageId}_options),""")
  }

  def printRawDatasets(schema: String,
                       tables: List[String],
                       storageId: String = "red_raw",
                       format: String = "DELTA",
                       loadType: String = "Insert"): Unit = {

    val idPadTo = tables.map(t => externalDsId(schema, t, 0).length).max + 1
    val pathPadTo = tables.map(t => s"/$schema/${camel2Snake(t)}".length).max + 1
    val tablePadTo = tables.map(_.length).max + 1
    tables.foreach(table =>
      printRawDataset(schema, table, storageId, format, loadType, idPadTo, pathPadTo, tablePadTo)
    )
  }

  def printRawDataset(schema: String,
                      tableName: String,
                      storageId: String = "red_raw",
                      format: String = "DELTA",
                      loadType: String = "Insert",
                      idPadTo: Int = 55,
                      pathPadTo: Int = 55,
                      tablePadTo: Int = 35): Unit = {

    val lcSchema = schema.toLowerCase
    val id = rawDsId(schema, tableName, idPadTo)
    val path = s""""/$lcSchema/${camel2Snake(tableName)}"""".padTo(pathPadTo, " ").mkString
    val table = s""""${lcSchema}_${camel2Snake(tableName)}"""".padTo(tablePadTo, " ").mkString
    println(s"""DatasetConf($id, $storageId, $path, $format, $loadType, Some(TableConf("raw", $table))),""")
  }

  def externalToRawMap(schema: String,
                       tableName: String,
                       idPadTo: Int = 55): Unit = {
    val sourceId = externalDsId(schema, tableName, idPadTo)
    val destinationId = rawDsId(schema, tableName, idPadTo)
    println(s""" $sourceId-> $destinationId,""")
  }


  def analyseColumn(df: DataFrame,
                    c: String): DataFrame = {
    val totalCount = df.select(c).count
    val countDistinct = df.select(c).distinct.count
    val countNull = df.select(c).where(col(c).isNull).count
    df.select(c).groupBy(c).count.orderBy(col("count").desc).limit(5)
      .withColumn(c, when(col(c).isNull, lit("null")).otherwise(col(c).cast(StringType)))
      .withColumn("%", bround(col("count")/lit(totalCount)*100, 3))
      .withColumn("column_name", lit(c))
      .groupBy("column_name")
      .agg(collect_list(struct(col(c) as "name", col("%"))) as s"%_top5_values")
      .withColumn("distinct_values", lit(countDistinct))
      .withColumn("total_values", lit(totalCount))
      .withColumn("%_null", bround(lit(countNull)/lit(totalCount)*100, 3))
      .withColumn("%_non_null", bround(lit(totalCount - countNull)/lit(totalCount)*100, 3))
      .withColumn("interpretation",
        when(col("distinct_values") === col("total_values"), lit("PK"))
          .when(lit(countDistinct) === 2 and lit(countNull) === 0 or lit(countDistinct) === 3 and lit(countNull) > 0, lit("BOOLEAN"))
          .otherwise(""))
  }

  def analyseDf(df: DataFrame): String = {
    import df.sparkSession.implicits._

    val head: String = df.columns.head
    val tail: Array[String] = df.columns.tail
    val resultDf = tail.zipWithIndex.foldLeft(analyseColumn(df, head)) { case (d, (c, idx)) =>
      log.info(s"ANALYSING [${c.padTo(50, " ").mkString}] \t COLUMN ${idx} out of ${df.columns.length} COLUMNS")
      d.unionByName(analyseColumn(df, c))
    }.as[(String, List[(String, Double)], Long, Long, Double, Double, String)]

    s"""${resultDf.columns.mkString(";")}
       |${resultDf.collect().map {case (a, b, c, d, e, f, g) => (a,b.mkString(","), c, d, e, f, g)}.map(_.productIterator.mkString(";")).mkString("\n")}
       |""".stripMargin
  }


}
