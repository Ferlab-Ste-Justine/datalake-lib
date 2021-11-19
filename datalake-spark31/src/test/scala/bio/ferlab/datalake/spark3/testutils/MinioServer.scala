package bio.ferlab.datalake.spark3.testutils

import org.scalatest.{BeforeAndAfterAll, TestSuite}
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, DeleteObjectRequest, ListObjectsRequest, PutObjectRequest}
import java.io.File

import bio.ferlab.datalake.spark3.file.{S3ClientBuilder, S3FileSystem}

import scala.collection.JavaConverters._
import scala.util.Random

trait MinioServer {
  private val minioPort = MinioContainer.startIfNotRunning()

  protected val minioEndpoint = s"http://localhost:${minioPort}"
  implicit val s3: S3Client = S3ClientBuilder.buildS3Client(pathStyleAccess = true, MinioContainer.accessKey, MinioContainer.secretKey, minioEndpoint, MinioContainer.region)
  implicit val s3FileSystem = new S3FileSystem(s3)

  val LOGGER: Logger = LoggerFactory.getLogger(getClass)
  val inputBucket = s"datal-import"
  val outputBucket = s"datal-repository"
  createBuckets()

  private def createBuckets(): Unit = {
    val alreadyExistingBuckets = s3.listBuckets().buckets().asScala.collect { case b if b.name() == inputBucket || b.name() == outputBucket => b.name() }
    val bucketsToCreate = Seq(inputBucket, outputBucket).diff(alreadyExistingBuckets)
    bucketsToCreate.foreach { b =>
      val buketRequest = CreateBucketRequest.builder().bucket(b).build()
      s3.createBucket(buketRequest)
    }
  }

  def withS3Objects[T](block: (String, String) => T): Unit = {
    val inputPrefix = s"run_${Random.nextInt(10000)}"
    LOGGER.info(s"Use input prefix $inputPrefix : $minioEndpoint/minio/$inputBucket/$inputPrefix")
    val outputPrefix = s"files_${Random.nextInt(10000)}"
    LOGGER.info(s"Use output prefix $outputPrefix : : $minioEndpoint/minio/$outputBucket/$outputPrefix")
    try {
      block(inputPrefix, outputPrefix)
    } finally {
      deleteRecursively(inputBucket, inputPrefix)
      deleteRecursively(outputBucket, outputPrefix)
    }
  }

  def list(bucket: String, prefix: String): Seq[String] = {
    val lsRequest = ListObjectsRequest.builder().bucket(bucket).prefix(prefix).build()
    s3.listObjects(lsRequest).contents().asScala.map(_.key())
  }

  private def deleteRecursively(bucket: String, prefix: String): Unit = {
    val lsRequest = ListObjectsRequest.builder().bucket(bucket).prefix(prefix).build()
    s3.listObjects(lsRequest).contents().asScala.foreach { o =>
      val del = DeleteObjectRequest.builder().bucket(bucket).key(prefix).build()
      s3.deleteObject(del)
    }
  }

  def ls(file: File): List[File] = {
    file.listFiles.filter(_.isFile).toList
  }

  def transferFromResources(prefix: String, resource: String, bucket: String = inputBucket): Unit = {
    val files = ls(new File(getClass.getResource(s"/$resource").toURI))
    files.foreach { f =>
      val put = PutObjectRequest.builder().bucket(bucket).key(s"$prefix/${f.getName}").build()
      s3.putObject(put, RequestBody.fromFile(f))
    }
  }

  def copyNFile(prefix: String, resource: String, times: Int, bucket: String = inputBucket): Unit = {
    val file = new File(getClass.getResource(s"/$resource").toURI)
    1.to(times).map { i =>
      val filename = s"${file.getName}_$i"
      val put = PutObjectRequest.builder().bucket(bucket).key(s"$prefix/$filename").build()
      s3.putObject(put, RequestBody.fromFile(file))
    }
  }
}


trait MinioServerSuite extends MinioServer with TestSuite with BeforeAndAfterAll {

}

object StartMinioServer extends App with MinioServer {
  LOGGER.info(s"Minio is started : $minioEndpoint")
  while (true) {

  }

}
