package bio.ferlab.datalake.spark3.file

import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileSystemSpec extends AnyFlatSpec with GivenWhenThen with Matchers with WithSparkSession {

  import spark.implicits._

  "extractPart" should "move a file outside of its folder" in {
    withOutputFolder("folder"){ tmp =>
      HadoopFileSystem.list(tmp, recursive = true).length shouldBe 0

      val destinationPath = s"$tmp/test"

      List("test")
        .toDF()
        .coalesce(1).write.mode("overwrite").csv(destinationPath)

      HadoopFileSystem.extractPart(destinationPath, "csv", "tsv")

      HadoopFileSystem.list(tmp, recursive = true).count(_.name.endsWith(".csv")) shouldBe 0
      HadoopFileSystem.list(tmp, recursive = true).count(_.name.endsWith(".tsv")) shouldBe 1

    }


  }

}
