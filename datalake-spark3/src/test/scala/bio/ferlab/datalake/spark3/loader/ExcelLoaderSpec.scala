package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.EXCEL
import bio.ferlab.datalake.spark3.testutils.AirportInput
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Paths}

class ExcelLoaderSpec extends SparkSpec {

  import spark.implicits._

  val folderPath: String = getClass.getClassLoader.getResource("raw/landing/").getPath
  val outputLocation: String = "output/airports.xlsx"
  val expected: Seq[AirportInput] = Seq(
    AirportInput("1", "YYC", "Calgary Int airport"),
    AirportInput("2", "YUL", "Montreal Int airport")
  )
  val expectedUpdate: Seq[AirportInput] = Seq(
    AirportInput("3", "YVR", "Vancouver Int airport")
  )

  val initialDF: DataFrame = expected.toDF()

  private def withInitialDfInFolder(rootPath: String)(testCode: String => Any): Unit = {
    val dfPath: String = rootPath + outputLocation
    ExcelLoader.writeOnce(dfPath, "", "", initialDF, Nil, EXCEL.sparkFormat, Map("header" -> "true"))
    testCode(dfPath)
  }

  private def withUpdatedDfInFolder(updates: DataFrame, path: String)(testCode: => Any): Unit = {
    ExcelLoader.insert(path, "", "", updates, Nil, EXCEL.sparkFormat, Map("header" -> "true"))
    testCode
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

    val options = Map.empty[String, String]
    val databaseName, tableName : Option[String] = None

    an[IllegalArgumentException] should be thrownBy {
      ExcelLoader.read(fileLocation, EXCEL.sparkFormat, options, databaseName, tableName)
    }
  }

  it should "read folder containing multiple Excel files as a DataFrame" in withOutputFolder("root") { root =>
    withInitialDfInFolder(root) { folderLocation =>
      withUpdatedDfInFolder(expectedUpdate.toDF(), folderLocation) {

        val result = ExcelLoader.read(folderLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

        result
          .as[AirportInput]
          .collect() should contain theSameElementsAs (expected ++ expectedUpdate)
      }
    }
  }

  "writeOnce" should "write a dataframe to a file" in withOutputFolder("root") { root =>
    withInitialDfInFolder(root) { folderLocation =>

      val result = ExcelLoader.read(folderLocation, EXCEL.sparkFormat, Map("header" -> "true"))

      result.as[AirportInput].collect() should contain theSameElementsAs expected

    }
  }

  it should "overwrite existing files when writing to the same folder" in withOutputFolder("root") { root =>
    withInitialDfInFolder(root) { folderLocation =>

      //Overwriting the same location
      ExcelLoader.writeOnce(outputLocation, "", "", expectedUpdate.toDF(), Nil, EXCEL.sparkFormat, Map("header" -> "true"))

      val result = ExcelLoader.read(folderLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

      result
        .as[AirportInput]
        .collect() should contain theSameElementsAs expectedUpdate
    }
  }

  "insert" should "append a dataframe to an existing file" in withOutputFolder("root") { root =>
    withInitialDfInFolder(root) { folderLocation =>
      withUpdatedDfInFolder(expectedUpdate.toDF(), folderLocation) {
        val result = ExcelLoader.read(folderLocation, EXCEL.sparkFormat, Map("header" -> "true"), None, None)

        result
          .as[AirportInput]
          .collect() should contain theSameElementsAs (expected ++ expectedUpdate)
      }
    }
  }

}
