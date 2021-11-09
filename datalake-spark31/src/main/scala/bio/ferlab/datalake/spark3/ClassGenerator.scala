package bio.ferlab.datalake.spark3

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io._
import java.time.LocalDateTime
import scala.annotation.tailrec

object ClassGenerator {

  def getType: PartialFunction[DataType, String] = {
    case StringType                           => "String"
    case FloatType                            => "Float"
    case IntegerType                          => "Int"
    case ShortType                            => "Short"
    case BooleanType                          => "Boolean"
    case DoubleType                           => "Double"
    case LongType                             => "Long"
    case DecimalType()                        => "Double"
    case DateType                             => "Date"
    case TimestampType                        => "Timestamp"
    case ArrayType(StringType, _)             => "List[String]"
    case ArrayType(FloatType, _)              => "List[Float]"
    case ArrayType(IntegerType, _)            => "List[Int]"
    case ArrayType(ShortType, _)              => "List[Short]"
    case ArrayType(BooleanType, _)            => "List[Boolean]"
    case ArrayType(DoubleType, _)             => "List[Double]"
    case ArrayType(LongType, _)               => "List[Long]"
    case MapType(StringType,StringType, _)    => "Map[String,String]"
    case MapType(StringType,LongType, _)      => "Map[String,Long]"
    case MapType(StringType,DecimalType(), _) => "Map[String,BigDecimal]"
    case MapType(StringType,ArrayType(StringType,_),_) =>"Map[String, List[String]]"
  }

  def getValue: PartialFunction[(String, Row, DataType), String] = {
    case (name, values, StringType)                                    => "\"" + values.getAs(name) + "\""
    case (name, values, DateType)                                      => s"""Date.valueOf("${values.getAs(name)}")"""
    case (name, values, TimestampType)                                 => s"""Timestamp.valueOf("${values.getAs(name)}")"""
    case (name, values, ArrayType(StringType,_)) if values.getAs[List[String]](name).isEmpty => "List()"
    case (name, values, ArrayType(StringType,_))                       => values.getAs[List[String]](name).mkString("List(\"", "\", \"", "\")")
    case (name, values, ArrayType(FloatType,_))                        => values.getAs[List[Float]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(IntegerType,_))                      => values.getAs[List[Int]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(BooleanType,_))                      => values.getAs[List[Boolean]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(DoubleType,_))                       => values.getAs[List[Boolean]](name).mkString("List(", ", ", ")")
    case (name, values, ArrayType(LongType,_))                         => values.getAs[List[Long]](name).mkString("List(", ", ", ")")
    case (name, values, MapType(StringType,StringType, _))             => values.getAs[Map[String, String]](name).map{ case (k, v) => s"""\"$k\" -> \"$v\"""" }.mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,LongType, _))               => values.getAs[Map[String, Long]](name).mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,DecimalType(), _))          => values.getAs[Map[String, BigDecimal]](name).mkString("Map(", ", ", ")")
    case (name, values, MapType(StringType,ArrayType(StringType,_),_)) => values.getAs[Map[String, List[String]]](name).mkString("Map(", ", ", ")")
    case (name, values, _)                                             => values.getAs(name)
  }

  def oneClassString(className: String, df: DataFrame): String = {
    if(df.isEmpty)
      throw new IllegalArgumentException("input dataframe empty.")

    val countNulls: Column = df.schema.fieldNames.map(c => functions.when(col(c).isNull, 1).otherwise(0)).reduce((a, b) => a + b)
    val values: Row = df.withColumn("countNulls", countNulls).orderBy(asc("countNulls")).drop("countNulls").head()
    val fields: Array[String] = {
      df.schema.fields.map {
        case StructField(name, dataType, _, _) if getValue.isDefinedAt(name, values, dataType) && getType.isDefinedAt(dataType)=>
          if(values.getAs(name) == null)
            s"""`$name`: Option[${getType(dataType)}] = None"""
          else
            s"""`$name`: ${getType(dataType)} = ${getValue(name, values, dataType)}"""

        case StructField(name, StructType(_), _, _) => s"""`$name`: ${name.toUpperCase} = ${name.toUpperCase}()"""
        case StructField(name, ArrayType(StructType(_), _), _, _) => s"""`$name`: List[${name.toUpperCase}] = List(${name.toUpperCase}())"""
        case structField: StructField => structField.toString()

      }
    }

    val spacing = s"case class $className(".toCharArray.map(_ => " ")

    s"""
case class $className(${fields.mkString("", s",\n${spacing.mkString}" , ")")}"""
  }

  private def getNestedClasses: DataFrame => List[String] = {df =>

    @tailrec
    def getNestedRecurse(done: Map[String, DataFrame], todo: List[DataFrame]): Map[String, DataFrame] = {
      todo match {
        case Nil => done
        case head :: tail =>
          val toAdd: Map[String, DataFrame] = head.schema.fields.collect {
            case StructField(name, StructType(_), _, _) => name.toUpperCase() -> head.select(s"${name}.*")
            case StructField(name, ArrayType(StructType(_), _), _, _) =>
              name.toUpperCase() -> head.withColumn(name, explode(col(name))).select(s"${name}.*")
          }.toMap
          getNestedRecurse(done ++ toAdd, tail ::: toAdd.values.toList)
      }
    }

    getNestedRecurse(Map(), List(df))
      .map { case (className, df) => oneClassString(className, df)}.toList

  }

  def getCaseClassFileContent(packageName: String, className: String, df: DataFrame): String = {

    val mainClass = oneClassString(className, df)

    val nestedClasses = getNestedClasses(df)

    s"""/**
       | * Generated by [[${this.getClass.getCanonicalName.replace("$", "")}]]
       | * on ${LocalDateTime.now()}
       | */
       |package $packageName
       |
       |$mainClass
       |${nestedClasses.mkString("\n")}
       |""".stripMargin
  }

  def writeCLassFile(packageName: String,
                     className: String,
                     df: DataFrame,
                     rootFolder: String = getClass.getResource(".").getFile): Unit = {

    val classContent = getCaseClassFileContent(packageName, className, df)
    val path: String = (rootFolder + packageName.replace(".", "/")+ s"/$className.scala")

    println(
      s"""writting file: $path :
         |$classContent
         |""".stripMargin)
    val file = new File(path)
    file.createNewFile()
    val pw = new PrintWriter(file)
    pw.write(classContent)
    pw.close()

  }


  def printCaseClassFromDataFrame(packageName: String, className: String, df: DataFrame): Unit = {
    println(getCaseClassFileContent(packageName, className, df))
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

