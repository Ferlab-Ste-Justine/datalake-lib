package bio.ferlab.datalake.spark3.file

import bio.ferlab.datalake.spark3.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}


class HadoopFileSystemSpec extends AnyFlatSpec with GivenWhenThen with Matchers with WithSparkSession {

  import spark.implicits._

  "extractPart" should "move a file outside of its folder" in {
    withOutputFolder("folder") { tmp =>
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

  "list" should "list files and subdirectories" in {
    withOutputFolder("root") { root =>
      HadoopFileSystem.list(root, recursive = true).length shouldBe 0

      /* Create the following file structure
      root
      ├── file1
      └── dir1
          └── dir2
              └── file2
       */
      val rootPath = Paths.get(root)
      Files.createFile(rootPath.resolve("file1").toAbsolutePath)
      Files.createDirectory(rootPath.resolve("dir1").toAbsolutePath)
      Files.createDirectory(rootPath.resolve("dir1/dir2").toAbsolutePath)
      Files.createFile(rootPath.resolve("dir1/dir2/file2").toAbsolutePath)

      val nonRecursiveResult = HadoopFileSystem.list(root, recursive = false)
      val recursiveResult = HadoopFileSystem.list(root, recursive = true)

      nonRecursiveResult.length shouldBe 2
      nonRecursiveResult.map(r => r.name -> r.isDir) should contain theSameElementsAs Map(
        "file1" -> false,
        "dir1" -> true
      )

      recursiveResult.length shouldBe 4
      recursiveResult.map(r => r.name -> r.isDir) should contain theSameElementsAs Map(
        "file1" -> false,
        "dir1" -> true,
        "dir2" -> true,
        "file2" -> false,
      )
    }
  }
}
