package bio.ferlab.datalake.spark3.file

import java.net.URI

import bio.ferlab.datalake.spark3.testutils.{MinioServerSuite, WithSparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class S3FileSystemSpec extends AnyFlatSpec with MinioServerSuite with Matchers with WithSparkSession {


  "isDirectory" should "return True if the path is a directory" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(prefix, "testS3")

      s3FileSystem.isDirectory(s"s3://$inputBucket/$prefix") shouldBe true

      s3FileSystem.isDirectory(s"s3://$inputBucket/$prefix/file1.txt") shouldBe false
    }

  }


  "exists" should "return true if file exists" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(prefix, "testS3")

      s3FileSystem.exists(s"s3://$inputBucket/$prefix") shouldBe true

      s3FileSystem.exists(s"s3://$inputBucket/$prefix/file1.txt") shouldBe true

      s3FileSystem.exists(s"s3://$inputBucket/$prefix/file3.txt") shouldBe false
    }

  }

  "list" should "return a list of files" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(s"$prefix/testS3", "testS3")
      transferFromResources(s"$prefix/testS3/subTestS3", "testS3/subTestS3")

      val files = s3FileSystem
        .list(s"s3://$inputBucket/$prefix/testS3", false)
        .map(element => element.path)

      val expected = List(
        s"s3://$inputBucket/$prefix/testS3/file1.txt",
        s"s3://$inputBucket/$prefix/testS3/file2.txt"
      )

      files shouldBe expected

    }
  }

  "list" should "return a list of files recursively" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(s"$prefix/testS3", "testS3")
      transferFromResources(s"$prefix/testS3/subTestS3", "testS3/subTestS3")

      val files = s3FileSystem
        .list(s"s3://$inputBucket/$prefix/testS3", true)
        .map(element => element.path)

      val expected = List(
        s"s3://$inputBucket/$prefix/testS3/file1.txt",
        s"s3://$inputBucket/$prefix/testS3/file2.txt",
        s"s3://$inputBucket/$prefix/testS3/subTestS3/file11.txt"
      )

      files shouldBe expected

    }
  }

  "copy" should "copy a file" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(s"$prefix/testS3", "testS3")
      transferFromResources(s"$prefix/testS3/subTestS3", "testS3/subTestS3")

      s3FileSystem.copy(
        s"s3://$inputBucket/$prefix/testS3/file1.txt",
        s"s3://$inputBucket/$prefix/testS3/subTestS3_2/file1.txt",
        false)

      s3FileSystem.exists(s"s3://$inputBucket/$prefix/testS3/subTestS3_2/file1.txt") shouldBe true
    }

  }

  "copy" should "copy a directory" in {
    withS3Objects { (prefix, _) =>
      transferFromResources(s"$prefix/testS3", "testS3")
      transferFromResources(s"$prefix/testS3/subTestS3", "testS3/subTestS3")

      s3FileSystem.copy(
        s"s3://$inputBucket/$prefix/testS3",
        s"s3://$inputBucket/$prefix/testS3/subTestS3_2",
        false)

      s3FileSystem.exists(s"s3://$inputBucket/$prefix/testS3/subTestS3_2") shouldBe true
      s3FileSystem.exists(s"s3://$inputBucket/$prefix/testS3/subTestS3_2/file1.txt") shouldBe true
    }

  }

  "remove" should "delete a file " in {
    withS3Objects { (prefix, _) =>
      transferFromResources(s"$prefix/testS3", "testS3")

      s3FileSystem.remove(s"s3://$inputBucket/$prefix/testS3/file1.txt")

      s3FileSystem.exists(s"s3://$inputBucket/$prefix/testS3/file1.txt") shouldBe false

    }

  }

  "remove" should "delete a directory " in {
    withS3Objects { (prefix, _) =>
      transferFromResources(s"$prefix/testS3", "testS3")

      s3FileSystem.remove(s"s3://$inputBucket/$prefix/testS3")

      s3FileSystem.exists(s"s3://$inputBucket/$prefix/testS3") shouldBe false

    }

  }

}
