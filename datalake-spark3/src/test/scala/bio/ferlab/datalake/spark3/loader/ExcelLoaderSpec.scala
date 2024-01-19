package bio.ferlab.datalake.spark3.loader

import bio.ferlab.datalake.commons.config.Format.EXCEL
import bio.ferlab.datalake.spark3.testutils.AirportInput
import bio.ferlab.datalake.testutils.SparkSpec

class ExcelLoaderSpec extends SparkSpec {

  import spark.implicits._

  val folderPath: String = getClass.getClassLoader.getResource("raw/landing/").getPath
  val expected: Seq[AirportInput] = Seq(
    AirportInput("1", "YYC", "Calgary Int airport"),
    AirportInput("2", "YUL", "Montreal Int airport")
  )

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


}
