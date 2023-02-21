package bio.ferlab.datalake.spark3.utils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j

import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import scala.annotation.tailrec

import org.apache.spark.sql.{DataFrame, Row}
import scala.util.Random
import scala.math.BigDecimal
object ClassGenerator {

  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def getType: PartialFunction[DataType, String] = {
    case StringType                           => "String"
    case FloatType                            => "Float"
    case IntegerType                          => "Int"
    case ShortType                            => "Short"
    case BooleanType                          => "Boolean"
    case DoubleType                           => "Double"
    case LongType                             => "Long"
    case DecimalType()                        => "BigDecimal"
    case DateType                             => "Date"
    case TimestampType                        => "Timestamp"
    case BinaryType                           => "Array[Byte]"
    case ArrayType(StringType, _)             => "Seq[String]"
    case ArrayType(FloatType, _)              => "Seq[Float]"
    case ArrayType(IntegerType, _)            => "Seq[Int]"
    case ArrayType(ShortType, _)              => "Seq[Short]"
    case ArrayType(BooleanType, _)            => "Seq[Boolean]"
    case ArrayType(DoubleType, _)             => "Seq[Double]"
    case ArrayType(LongType, _)               => "Seq[Long]"
    case MapType(StringType,StringType, _)    => "Map[String,String]"
    case MapType(StringType,LongType, _)      => "Map[String,Long]"
    case MapType(StringType,DecimalType(), _) => "Map[String,BigDecimal]"
    case MapType(StringType,ArrayType(StringType,_),_) =>"Map[String, Seq[String]]"
  }

  val maxElement: Int = 100

  def getValue: PartialFunction[(String, Row, DataType), String] = {
    case (name, values, StringType)                                       => "\"" + values.getAs[String](name).replace("\"", "\\\"").replaceAll("\n", " ").take(512) + "\""
    case (name, values, DateType)                                         => s"""java.sql.Date.valueOf("${values.getAs(name)}")"""
    case (name, values, TimestampType)                                    => s"""java.sql.Timestamp.valueOf("${values.getAs(name)}")"""
    case (name, values, BinaryType)                                       => values.getAs[Array[Byte]](name).take(maxElement).map(b => s"$b.toByte").mkString("Array(", ", ", ")")
    case (name, values, FloatType)                                        => s"""${values.getAs(name)}f"""
    case (name, values, ArrayType(StringType,_))
      if Option(values.getAs[Seq[String]](name)).getOrElse(Seq()).isEmpty => "Seq()" // If the array is empty ==> Seq(), or undefined ==> null
    case (name, values, ArrayType(StringType,_))                          => values.getAs[Seq[String]](name).take(maxElement).mkString("Seq(\"", "\", \"", "\")")
    case (name, values, ArrayType(FloatType,_))                           => values.getAs[Seq[Float]](name).take(maxElement).mkString("Seq(", ", ", ")")
    case (name, values, ArrayType(IntegerType,_))                         => values.getAs[Seq[Int]](name).take(maxElement).mkString("Seq(", ", ", ")")
    case (name, values, ArrayType(BooleanType,_))                         => values.getAs[Seq[Boolean]](name).take(maxElement).mkString("Seq(", ", ", ")")
    case (name, values, ArrayType(DoubleType,_))                          => values.getAs[Seq[Double]](name).take(maxElement).mkString("Seq(", ", ", ")")
    case (name, values, ArrayType(LongType,_))                            => values.getAs[Seq[Long]](name).take(maxElement).mkString("Seq(", ", ", ")")
    case (name, values, MapType(StringType,StringType, _))                => values.getAs[Map[String, String]](name).take(maxElement).map{ case (k, v) => s"""\"$k\" -> \"$v\"""" }.mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,LongType, _))                  => values.getAs[Map[String, Long]](name).take(maxElement).mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,DecimalType(), _))             => values.getAs[Map[String, BigDecimal]](name).take(maxElement).mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,ArrayType(StringType,_),_))    => values.getAs[Map[String, Seq[String]]](name).take(maxElement).mkString("Map(", ", ", ")")
    case (name, values, _)                                                => values.getAs(name)
  }

  def oneClassString(className: String, df: DataFrame): String = {

    val fields: Array[String] = if(df.isEmpty) {
      df.schema.fields.map {
        case StructField(name, dataType, _, _) => s"""`$name`: Option[${getType(dataType)}] = None"""
        case StructField(name, StructType(_), _, _) => s"""`$name`: ${name.toUpperCase}"""
        case StructField(name, ArrayType(StructType(_), _), _, _) => s"""`$name`: Seq[${name.toUpperCase}]"""
        case structField: StructField => structField.toString()
      }
    } else {
      val countNulls: Column = df.schema.fieldNames.map(c => functions.when(col(c).isNull, 1).otherwise(0)).reduce((a, b) => a + b)
      val values: Row = df.withColumn("countNulls", countNulls).orderBy(asc("countNulls")).drop("countNulls").head()
      df.schema.fields.map {
        case StructField(name, dataType, _, _) if getValue.isDefinedAt(name, values, dataType) && getType.isDefinedAt(dataType)=>
          if(values.getAs(name) == null)
            s"""`$name`: Option[${getType(dataType)}] = None"""
          else
            s"""`$name`: ${getType(dataType)} = ${getValue(name, values, dataType)}"""
        case StructField(name, StructType(_), _, _) => s"""`$name`: ${name.toUpperCase} = ${name.toUpperCase}()"""
        case StructField(name, ArrayType(StructType(_), _), _, _) => s"""`$name`: Seq[${name.toUpperCase}] = Seq(${name.toUpperCase}())"""
        case structField: StructField => structField.toString()
      }
    }

    val spacing = s"case class $className(".toCharArray.map(_ => " ")

    s"""
case class $className(${fields.mkString("", s",\n${spacing.mkString}" , ")")}"""
  }

  private def getNestedClasses: DataFrame => Seq[String] = {df =>

    @tailrec
    def getNestedRecurse(done: Map[String, DataFrame], todo: Seq[DataFrame]): Map[String, DataFrame] = {
      todo match {
        case Nil => done
        case head :: tail =>
          val toAdd: Map[String, DataFrame] = head.schema.fields.collect {
            case StructField(name, StructType(_), _, _) => name.toUpperCase() -> head.select(s"${name}.*")
            case StructField(name, ArrayType(StructType(_), _), _, _) =>
              name.toUpperCase() -> head.withColumn(name, explode(col(name))).select(s"${name}.*")
          }.toMap
          getNestedRecurse(done ++ toAdd, tail ++ toAdd.values.toSeq)
      }
    }

    getNestedRecurse(Map(), Seq(df))
      .map { case (className, df) => oneClassString(className, df)}.toSeq

  }

  def getCaseClassFileContent(packageName: String,
                              className: String,
                              df: DataFrame,
                              on: LocalDateTime = LocalDateTime.now()): String = {

    val mainClass = oneClassString(className, df)

    val nestedClasses = getNestedClasses(df)

    val imports: Seq[String] =
      if ((mainClass + nestedClasses.mkString("\n")).contains("Timestamp") && (mainClass + nestedClasses.mkString("\n")).contains("Date"))
        Seq("import java.sql.{Date, Timestamp}")
      else if ((mainClass + nestedClasses.mkString("\n")).contains("Timestamp"))
        Seq("import java.sql.Timestamp")
      else if ((mainClass + nestedClasses.mkString("\n")).contains("Date"))
        Seq("import java.sql.Date")
      else Seq()

    s"""/**
       | * Generated by [[${this.getClass.getCanonicalName.replace("$", "")}]]
       | * on ${on}
       | */
       |package $packageName
       |
       |${imports.mkString("\n")}
       |
       |$mainClass
       |${nestedClasses.mkString("\n")}
       |""".stripMargin
  }

  def getRandomValue(dataType: org.apache.spark.sql.types.DataType): Any = dataType match {
    case _: StringType => Random.alphanumeric.take(10).mkString
    case _: IntegerType => Math.abs(Random.nextInt())
    case _: DoubleType => Math.abs(Random.nextDouble())
    case _: BooleanType => Random.nextBoolean()
    case _: DateType => java.sql.Date.valueOf("2022-02-20")
    case _: TimestampType => java.sql.Timestamp.valueOf("2022-02-20 12:34:56.789")
    case _: DecimalType => BigDecimal.valueOf(Math.abs(Random.nextInt()))
    case _: LongType => Math.abs(Random.nextLong())
    case _: ShortType => Math.abs(Random.nextInt().toShort)
    case _ => null
  }

  def writeCLassFile(
                      packageName: String,
                      className: String,
                      df: DataFrame,
                      rootFolder: String = getClass.getResource(".").getFile,
                      randomizeValues: Boolean = false
                    ): Unit = {
    val dfToWrite = if (randomizeValues) {
      val schema = df.schema
      val randomData = Seq(Row.fromSeq(schema.fields.map { field =>
        getRandomValue(field.dataType)
      }))
      df.sparkSession.createDataFrame(df.sparkSession.sparkContext.parallelize(randomData), schema)
    } else {
      df
    }

    val classContent = getCaseClassFileContent(packageName, className, dfToWrite)
    val folder = s"""$rootFolder${packageName.replace(".", "/")}"""
    val path: String = s"$folder/$className.scala"

    log.info(
      s"""writing file: $path :
         |$classContent
         |""".stripMargin)
    val file = new File(path)

    val directory = new File(folder)

    if (!directory.exists) {
      directory.mkdirs()
    }

    file.createNewFile()
    val pw = new PrintWriter(file)
    pw.write(classContent)
    pw.close()
  }


  def printCaseClassFromDataFrame(packageName: String, className: String, df: DataFrame): Unit = {
    log.info(getCaseClassFileContent(packageName, className, df))
  }
}

object ClassGeneratorImplicits {
  implicit class ClassGenDataFrameOps(df: DataFrame) {
    def writeCLassFile(packageName: String,
                       className: String,
                       rootFolder: String): Unit =
      ClassGenerator.writeCLassFile(packageName, className, df, rootFolder)
  }
}


