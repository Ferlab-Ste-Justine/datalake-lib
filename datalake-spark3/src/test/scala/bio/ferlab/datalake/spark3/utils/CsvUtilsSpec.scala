package bio.ferlab.datalake.spark3.utils

import bio.ferlab.datalake.commons.config.{DatasetConf, FixedRepartition, SimpleConfiguration}
import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA}
import bio.ferlab.datalake.commons.config.LoadType.{Insert, OverWrite, OverWritePartition}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.loader.LoadResolver
import bio.ferlab.datalake.spark3.testutils.{AirportInput, WithTestConfig}
import bio.ferlab.datalake.testutils.SparkSpec
import org.apache.spark.sql.types.DateType

class CsvUtilsSpec extends SparkSpec with WithTestConfig {
  import spark.implicits._

  val destinationDs: DatasetConf = DatasetConf("destination", alias, "/destination", CSV, OverWrite, repartition = Some(FixedRepartition(1)), readoptions = Map("inferSchema" -> "true", "header" -> "true"), writeoptions = Map("header" -> "true"))
  override lazy implicit val conf: SimpleConfiguration = sc.copy(datalake = sc.datalake.copy(sources = List(destinationDs)))

  val inputData = Seq(
    AirportInput("1", "YYC", "Calgary Int airport"),
    AirportInput("2", "YUL", "Montreal Int airport")
  )


  "renameCsvFile" should "rename file if the format is CSV and data is repartitioned into a single file" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val fileName = "destination.csv"
      LoadResolver
        .write(spark, updatedConf)(destinationDs.format -> destinationDs.loadtype)
        .apply(destinationDs, destinationDs.repartition.get.repartition(inputData.toDF()))

      val resultDf = CsvUtils.renameCsvFile(destinationDs)(spark, updatedConf)
      resultDf
        .as[AirportInput]
        .collect() should contain theSameElementsAs inputData

      val files = FileSystemResolver
        .resolve(updatedConf.getStorage(destinationDs.storageid).filesystem)
        .list(destinationDs.location(updatedConf), recursive = true)

      files.size shouldBe 1
      files.head.name shouldBe fileName
    }
  }

  "renameCsvFile" should "preserve the existing files in the destination if load type is insert" in {
    withOutputFolder("root") { root =>
      val updatedConf = updateConfStorages(conf, root)

      val insertDestinationDs = destinationDs.copy(loadtype = Insert)
      val fs = FileSystemResolver.resolve(updatedConf.getStorage(insertDestinationDs.storageid).filesystem)
      val load = LoadResolver.write(spark, updatedConf)(insertDestinationDs.format -> insertDestinationDs.loadtype)

      // Existing data
      val existingData = inputData
      load(insertDestinationDs, insertDestinationDs.repartition.get.repartition(existingData.toDF()))

      val existingDf = CsvUtils.renameCsvFile(insertDestinationDs, suffix = Some("v1"))(spark, updatedConf)
      existingDf
        .as[AirportInput]
        .collect() should contain theSameElementsAs existingData

      val existingFiles = fs.list(insertDestinationDs.location(updatedConf), recursive = true)
      existingFiles.size shouldBe 1
      existingFiles.head.name shouldBe "destination_v1.csv"

      // New data
      val newData = Seq(
        AirportInput("3", "YYZ", "Toronto Int airport"),
        AirportInput("4", "YVR", "Vancouver Int airport")
      )
      load(insertDestinationDs, insertDestinationDs.repartition.get.repartition(newData.toDF()))

      val newDf = CsvUtils.renameCsvFile(insertDestinationDs, suffix = Some("v2"))(spark, updatedConf)
      newDf
        .as[AirportInput]
        .collect() should contain theSameElementsAs existingData ++ newData

      val newFiles = fs.list(insertDestinationDs.location(updatedConf), recursive = true)
      newFiles.size shouldBe 2
      newFiles.count(_.name === "destination_v1.csv") shouldBe 1
      newFiles.count(_.name === "destination_v2.csv") shouldBe 1
    }
  }

}
