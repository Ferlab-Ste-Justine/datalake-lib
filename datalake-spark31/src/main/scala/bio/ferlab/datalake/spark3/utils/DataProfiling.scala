package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.spark3.transformation.CamelToSnake.camel2Snake
import bio.ferlab.datalake.spark3.transformation.NormalizeColumnName.replace_special_char_by_underscore
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.slf4j

object DataProfiling {

  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def initSparkSession(s3accessKey: String,
                       s3secretKey: String,
                       s3Endpoint: String,
                       existingSparkSession: SparkSession): SparkSession = {
    val existingConf = existingSparkSession.sparkContext.getConf
    existingSparkSession.stop()

    val conf = Map(
      "spark.sql.legacy.timeParserPolicy" -> "CORRECTED",
      "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" -> "CORRECTED",
      "fs.s3a.access.key" -> s3accessKey,
      "fs.s3a.secret.key" -> s3secretKey,
      "spark.hadoop.fs.s3a.endpoint" -> s3Endpoint,
      "spark.hadoop.fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "spark.hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "spark.hadoop.fs.s3a.path.style.access" -> "true",
      "spark.hadoop.fs.s3a.connection.ssl.enabled" -> "true",
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
      "spark.delta.merge.repartitionBeforeWrite" -> "true"
    )

    val sparkConf: SparkConf = conf.foldLeft(existingConf){ case (c, (k, v)) => c.set(k, v) }

    SparkSession
      .builder
      .config(sparkConf)
      .enableHiveSupport()
      .appName("SparkApp")
      .getOrCreate()
  }

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
                         folder: String = "s3a://red-prd/raw",
                         format: String = "DELTA",
                         loadType: String = "INSERT"): Unit = {

    val lcSchema = schema.toLowerCase
    println(s"$schema.$tableName,${format},${folder}/$systemName/${camel2Snake(tableName)},${lcSchema}_${camel2Snake(tableName)},$loadType")
    df.schema.fields.foreach{
      case StructField(name, dataType, _, _) =>
        val normalizedName = replace_special_char_by_underscore(name)
        val spec = s"$name,${ClassGenerator.getType(dataType)},-,$normalizedName,${ClassGenerator.getType(dataType)}"
        if(normalizedName!=name) println(s"$spec,NormalizeColumnName")
        else println(spec)
    }
  }

  def externalDsId(schema: String, tableName: String, idPadTo: Int = 55): String = {
    s""""${schema}.${tableName}"""".padTo(idPadTo, " ").mkString
  }

  def rawDsId(schema: String, tableName: String, idPadTo: Int = 55): String = {
    s""""raw_${schema.toLowerCase}_${tableName.toLowerCase}"""".padTo(idPadTo, " ").mkString
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
