package bio.ferlab.datalake.commons.file

import bio.ferlab.datalake.commons.config.testutils.WithOutputFolder
import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}


class HadoopFileSystemSpec extends AnyFlatSpec with Matchers with WithOutputFolder {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

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

  "copy" should "copy a file to the specified location" in {
    withOutputFolder("root") { root =>
      HadoopFileSystem.list(root, recursive = true).length shouldBe 0

      val rootPath = Paths.get(root)
      Files.createFile(rootPath.resolve("myFile").toAbsolutePath)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 1

      HadoopFileSystem.copy(
        rootPath.resolve("myFile").toAbsolutePath.toString,
        rootPath.resolve("myNewFile").toAbsolutePath.toString,
        overwrite = false)

      val files = HadoopFileSystem.list(root, recursive = true)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 2
      files.count(_.name === "myFile") shouldBe 1 // old file still exists
      files.count(_.name === "myNewFile") shouldBe 1 // new file has been created
    }
  }

  it should "not overwrite destination if overwrite is false" in {
    withOutputFolder("root") { root =>
      HadoopFileSystem.list(root, recursive = true).length shouldBe 0

      val rootPath = Paths.get(root)
      val content = "hello world"

      Files.writeString(rootPath.resolve("myFile").toAbsolutePath, content)
      Files.createFile(rootPath.resolve("myExistingFile").toAbsolutePath)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 2
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe empty

      an[FileAlreadyExistsException] shouldBe thrownBy {
        HadoopFileSystem.copy(
          rootPath.resolve("myFile").toAbsolutePath.toString,
          rootPath.resolve("myExistingFile").toAbsolutePath.toString,
          overwrite = false)
      }
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe empty
    }
  }

  it should "overwrite destination if overwrite is true" in {
    withOutputFolder("root") { root =>
      HadoopFileSystem.list(root, recursive = true).length shouldBe 0

      val rootPath = Paths.get(root)
      val content = "hello world"
      val existingContent = "abcd"

      Files.writeString(rootPath.resolve("myFile").toAbsolutePath, content)
      Files.writeString(rootPath.resolve("myExistingFile").toAbsolutePath, existingContent)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 2
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe existingContent

      noException shouldBe thrownBy {
        HadoopFileSystem.copy(
          rootPath.resolve("myFile").toAbsolutePath.toString,
          rootPath.resolve("myExistingFile").toAbsolutePath.toString,
          overwrite = true)
      }
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe content
    }
  }

  "move" should "move a file to the specified location" in {
    withOutputFolder("root") { root =>
      HadoopFileSystem.list(root, recursive = true).length shouldBe 0

      val rootPath = Paths.get(root)
      Files.createFile(rootPath.resolve("myFile").toAbsolutePath)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 1

      HadoopFileSystem.move(
        rootPath.resolve("myFile").toAbsolutePath.toString,
        rootPath.resolve("myNewFile").toAbsolutePath.toString,
        overwrite = false)

      val files = HadoopFileSystem.list(root, recursive = true)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 1
      files.count(_.name === "myFile") shouldBe 0 // old file has been removed
      files.count(_.name === "myNewFile") shouldBe 1 // new file has been created
    }
  }

  it should "not overwrite destination if overwrite is false" in {
    withOutputFolder("root") { root =>
      HadoopFileSystem.list(root, recursive = true).length shouldBe 0

      val rootPath = Paths.get(root)
      val content = "hello world"

      Files.writeString(rootPath.resolve("myFile").toAbsolutePath, content)
      Files.createFile(rootPath.resolve("myExistingFile").toAbsolutePath)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 2
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe empty

      an[FileAlreadyExistsException] shouldBe thrownBy {
        HadoopFileSystem.move(
          rootPath.resolve("myFile").toAbsolutePath.toString,
          rootPath.resolve("myExistingFile").toAbsolutePath.toString,
          overwrite = false)
      }
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe empty
    }
  }

  it should "overwrite destination if overwrite is true" in {
    withOutputFolder("root") { root =>
      HadoopFileSystem.list(root, recursive = true).length shouldBe 0

      val rootPath = Paths.get(root)
      val content = "hello world"
      val existingContent = "abcd"

      Files.writeString(rootPath.resolve("myFile").toAbsolutePath, content)
      Files.writeString(rootPath.resolve("myExistingFile").toAbsolutePath, existingContent)
      HadoopFileSystem.list(root, recursive = true).length shouldBe 2
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe existingContent

      noException shouldBe thrownBy {
        HadoopFileSystem.move(
          rootPath.resolve("myFile").toAbsolutePath.toString,
          rootPath.resolve("myExistingFile").toAbsolutePath.toString,
          overwrite = true)
      }
      Files.readString(rootPath.resolve("myExistingFile").toAbsolutePath) shouldBe content
    }
  }
}
