package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.EXCEL
import bio.ferlab.datalake.spark3.testutils.AirportInput
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Paths}

class ExcelLoaderSpec extends SparkSpec with BeforeAndAfterEach {

  import spark.implicits._

  val folderPath: String = getClass.getClassLoader.getResource("raw/landing/").getPath
  val outputLocation: String = folderPath + "output/airports.xlsx"
  val expected: Seq[AirportInput] = Seq(
    AirportInput("1", "YYC", "Calgary Int airport"),
    AirportInput("2", "YUL", "Montreal Int airport")
  )
  val simpleExpectedUpdate: Seq[AirportInput] = Seq(
    AirportInput("3", "YVR", "Vancouver Int airport")
  )

  val initialDF: DataFrame = expected.toDF()

  override def afterEach(): Unit = {
    super.afterEach()
    val outputPath = Paths.get(outputLocation)
    if (Files.exists(outputPath)) {
      cleanUpFilesRecursively(outputPath)
    }
  }

  /**
   * Recursively deletes files and directories at the given path. Necessary because spark-excel format API v2
   * may create multiple excel partitions when writing to a folder.
   * */
  private def cleanUpFilesRecursively(path: java.nio.file.Path): Unit = {
    if (Files.isDirectory(path)) {
      Files.list(path).forEach(cleanUpFilesRecursively)
    }
    Files.deleteIfExists(path)
  }

  private def withInitialDfInFolder(testCode: => Any): Unit = {
    ExcelLoader.writeOnce(outputLocation, "", "", initialDF, Nil, EXCEL.sparkFormat, Map("header" -> "true"))
    testCode
  }

  private def withUpdatedDfInFolder(updates: DataFrame, testCode: String => Any): Unit = {
    ExcelLoader.insert(outputLocation, "", "", updates, Nil, EXCEL.sparkFormat, Map("header" -> "true"))
    testCode(outputLocation)
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

  it should "throw an exception when the header option is missing" in {
    val fileLocation: String = folderPath + "airports.xlsx"

    an[IllegalArgumentException] should be thrownBy {
      ExcelLoader.read(fileLocation, EXCEL.sparkFormat, Map.empty, None, None)
    }
  }

  it should "read folder containing multiple Excel files as a DataFrame" in withInitialDfInFolder {
    withUpdatedDfInFolder(simpleExpectedUpdate.toDF(), { folderLocation =>

      val result = ExcelLoader.read(folderLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

      result
        .as[AirportInput]
        .collect() should contain theSameElementsAs (expected ++ simpleExpectedUpdate)
    })
  }

  "writeOnce" should "write a dataframe to a file" in {
    ExcelLoader.writeOnce(outputLocation, "", "", initialDF, Nil, EXCEL.sparkFormat, Map("header" -> "true"))

    val result = ExcelLoader.read(outputLocation, EXCEL.sparkFormat, Map("header" -> "true"))

    result.as[AirportInput].collect() should contain theSameElementsAs expected
  }

  it should "overwrite existing files when writing to the same folder" in withInitialDfInFolder {
    //Overwriting the same location
    ExcelLoader.writeOnce(outputLocation, "", "", simpleExpectedUpdate.toDF(), Nil, EXCEL.sparkFormat, Map("header" -> "true"))

    val result = ExcelLoader.read(outputLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

    result
      .as[AirportInput]
      .collect() should contain theSameElementsAs simpleExpectedUpdate
  }

  "insert" should "append a dataframe to an existing file" in withInitialDfInFolder {
    withUpdatedDfInFolder(simpleExpectedUpdate.toDF(), {
      folderLocation =>
        val result = ExcelLoader.read(folderLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

        result
          .as[AirportInput]
          .collect() should contain theSameElementsAs (expected ++ simpleExpectedUpdate)
    })
  }

}
