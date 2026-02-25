package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.EXCEL
import bio.ferlab.datalake.spark3.testutils.AirportInput
import bio.ferlab.datalake.testutils.SparkSpec
import org.scalatest.BeforeAndAfterAll

import java.nio.file.{Files, Paths}

class ExcelLoaderSpec extends SparkSpec with BeforeAndAfterAll{

  import spark.implicits._

  val folderPath: String = getClass.getClassLoader.getResource("raw/landing/").getPath
  val outputLocation: String = folderPath + "output/airports.xlsx"
  val expected: Seq[AirportInput] = Seq(
    AirportInput("1", "YYC", "Calgary Int airport"),
    AirportInput("2", "YUL", "Montreal Int airport")
  )
  val initialDF = expected.toDF()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val outputPath = Paths.get(outputLocation)
    if (Files.exists(outputPath)) {
      Files.delete(outputPath)
    }
  }

  "read" should "read xlsx file as a DataFrame" in {
    val fileLocation = folderPath + "airports.xlsx"

    val result = ExcelLoader.read(fileLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

    result
      .as[AirportInput]
      .collect() should contain theSameElementsAs expected
  }

  it should "read xls file as a DataFrame" in {
    val fileLocation = folderPath + "airports.xls"

    val result = ExcelLoader.read(fileLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

    result
      .as[AirportInput]
      .collect() should contain theSameElementsAs expected
  }

  "writeOnce" should "write a dataframe to a file" in {
    ExcelLoader.writeOnce(outputLocation, "", "", initialDF, Nil, EXCEL.sparkFormat, Map("header" -> "true"))

    val result = ExcelLoader.read(outputLocation, EXCEL.sparkFormat, Map("header" -> "true"))

    result.as[AirportInput].collect() should contain theSameElementsAs expected
  }

  "insert" should "append a dataframe to an existing file" in {
    // Overwrite with initial data first
    ExcelLoader.writeOnce(outputLocation, "", "", initialDF, Nil, EXCEL.sparkFormat, Map("header" -> "true"))

    // Prepare new data and append it
    val updates = Seq(AirportInput("3", "YVR", "Vancouver Int airport")).toDF()
    val expectedDfValues = expected ++ Seq(AirportInput("3", "YVR", "Vancouver Int airport"))

    ExcelLoader.insert(outputLocation, "", "", updates, Nil, EXCEL.sparkFormat, Map("header" -> "true"))

    val result = ExcelLoader.read(outputLocation, EXCEL.sparkFormat, Map("header" -> "true"))

    result.as[AirportInput].collect() should contain theSameElementsAs expectedDfValues
  }


}
