package bio.ferlab.datalake.spark3.file
import bio.ferlab.datalake.commons.file.{File, FileSystem}
import org.slf4j
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model.{CopyObjectRequest, DeleteObjectRequest, HeadObjectRequest, ListObjectsV2Request, ListObjectsV2Response, NoSuchKeyException}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region

import java.net.URI
import java.nio.file.Paths


object S3ClientBuilder {

  val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  def buildS3Client(pathStyleAccess: Boolean, accessKey: String, secretKey: String, endPoint: String, region: String = "region"): S3Client = {
    val confBuilder: S3Configuration = software.amazon.awssdk.services.s3.S3Configuration.builder
      .pathStyleAccessEnabled(pathStyleAccess)
      .build
    val staticCredentialsProvider: StaticCredentialsProvider = StaticCredentialsProvider.create(
      AwsBasicCredentials.create(accessKey, secretKey)
    )
    val endpoint = URI.create(endPoint)
    val s3: S3Client = S3Client.builder
      .credentialsProvider(staticCredentialsProvider)
      .region(Region.of(region))
      .endpointOverride(endpoint)
      .serviceConfiguration(confBuilder)
      .httpClient(ApacheHttpClient.create)
      .build
    s3
  }

}


class S3FileSystem(s3Client: S3Client) extends FileSystem {
  val DELIMITER = "/"
  val S3_SCHEME = "s3://"

  private def sanitizePath(path: String): String = {
    var sPath  = path.replaceAll(DELIMITER + "+", DELIMITER)
    if (sPath.startsWith(DELIMITER) && path!= DELIMITER)
      sPath = path.substring(1)
    sPath
  }

  private def getBase(uri: URI) =  new URI(uri.getScheme, uri.getHost, null, null)

  private def isPathTerminatedByDelimiter(uri: URI) = uri.getPath.endsWith(DELIMITER)

  private def normalizeToDirectoryPrefix(uri: URI): String = {
    val strippedUri = getBase(uri).relativize(uri)
    if (isPathTerminatedByDelimiter(strippedUri))
      return sanitizePath(strippedUri.getPath)
    sanitizePath(strippedUri.getPath + DELIMITER)
  }

  private def normalizeToDirectoryUri(uri: URI): URI = {
    if (isPathTerminatedByDelimiter(uri)) return uri
    log.info(uri.getHost)
    new URI(uri.getScheme, uri.getHost, uri.getPath + DELIMITER, null)
  }

  /**
   * Determines if the given path is a directory
   *
   * @param path  path
   * @return {@code true} if path is a directory
   *         {@code false} otherwise
   */
  def isDirectory(path: String): Boolean = try {
    val uri = new URI(path)
    val prefix = normalizeToDirectoryPrefix(uri)
    if (prefix == DELIMITER) return true
    val listObjectsV2Request = ListObjectsV2Request.builder.bucket(uri.getHost).prefix(prefix).maxKeys(2).build()
    val listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request)
    listObjectsV2Response.hasContents
  } catch {
    case _: NoSuchKeyException =>
      false
  }

  /**
   * Determines if the path exists
   *
   * @param path a path (file or a directory)
   * @return {@code true} if the path exists
   *         {@code false} otherwise
   */
  def exists(path: String): Boolean = try {
    if (isDirectory(path)) return true
    val pathUri = new URI(path)
    if (isPathTerminatedByDelimiter(pathUri)) return false
    existsFile(pathUri)
  } catch {
    case _: NoSuchKeyException =>
      false
  }

  /**
   * Determines if the file exists at the given path
   *
   * @param uri file path
   * @return {@code true} if the file exists in the path
   *         {@code false} otherwise
   */
  private def existsFile(uri: URI) = try {
    val base = getBase(uri)
    val path = sanitizePath(base.relativize(uri).getPath)
    val headObjectRequest = HeadObjectRequest.builder.bucket(uri.getHost).key(path).build
    s3Client.headObject(headObjectRequest)
    true
  } catch {
    case _: NoSuchKeyException =>
      false
  }

  /**
   * List all files within a folder.
   *
   * @param path      where to list the files
   * @param recursive if true, list files in a recursive manner
   * @return a list of Files
   */

  override def list(path: String, recursive: Boolean): List[File] = {

    val uriPath = new URI(path)
    var continuationToken: String = null
    var fileKey = new String
    var isDone = false
    var files = List.empty[File]

    val prefix = normalizeToDirectoryPrefix(uriPath)
    while (!isDone) {
      var listObjectsV2RequestBuilder = ListObjectsV2Request.builder.bucket(uriPath.getHost)
      if (prefix != DELIMITER)
        listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(prefix)
      if (!recursive)
        listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.delimiter(DELIMITER)
      if (continuationToken != null)
        listObjectsV2RequestBuilder.continuationToken(continuationToken)

      val listObjectsV2Request = listObjectsV2RequestBuilder.build
      val listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request)
      val filesReturned = listObjectsV2Response.contents

      filesReturned.stream.forEach { element =>
        if (!element.key.equals(uriPath.getPath) && !element.key.endsWith(DELIMITER))
          fileKey = element.key
        if (fileKey.startsWith(DELIMITER))
          fileKey = fileKey.substring(1)
          files = files :+ File(S3_SCHEME + uriPath.getHost + DELIMITER + fileKey, fileKey, element.size, false)
      }

      isDone = !listObjectsV2Response.isTruncated
      continuationToken = listObjectsV2Response.nextContinuationToken
    }

    files
  }

  /**
   * Method to copy file from source to destination.
   *
   * @param source source path
   * @param destination destination path
   * @return {@code true} if the copy operation succeeds, i.e., response code is 200
   *         {@code false} otherwise
   */

  private def copyFile(source: String, destination: String)= {
    val uriSrcPath = new URI(source)
    val uriDstPath = new URI(destination)

    val copyReq = CopyObjectRequest
      .builder.sourceBucket(uriSrcPath.getHost)
      .sourceKey(sanitizePath(uriSrcPath.getPath))
      .destinationBucket(uriDstPath.getHost)
      .destinationKey(sanitizePath(uriDstPath.getPath))
      .build
    s3Client.copyObject(copyReq)

  }

  /**
   * Copy the content of a folder or a single file to a destination.
   *
   * @param source      the path to the source folder or file
   * @param destination the path to the destination folder or file
   * @param overwrite   If true, the destination path will be overwritten.
   *                    If false and the destination is not empty then the method will fail.
   */
  override def copy(source: String, destination: String, overwrite: Boolean): Unit = {

    if(exists(destination)){
      if (!overwrite){
        log.info("Failed to copy! Cannot overwrite existing destination")
        return
      }
      else
        remove(destination)
      }

    val uriDstPath = new URI(destination)
    val uriSrc = new URI(source)

  if (!isDirectory(source))
    copyFile(source, destination)
  else {
    val uriDst = normalizeToDirectoryUri(uriDstPath)
    val srcPath = Paths.get(uriSrc.getPath)

    for (filePath <- list(source, true)) {
      val srcFileURI = URI.create(filePath.path)
      val directoryEntryPrefix = srcFileURI.getPath

      val src = new URI(uriSrc.getScheme, uriSrc.getHost, directoryEntryPrefix, null)
      val relativeSrcPath = srcPath.relativize(Paths.get(directoryEntryPrefix)).toString
      val dst = uriDst.resolve(relativeSrcPath).toString


      copyFile(src.toString, dst)
    }
  }

  }

  /**
   * Move the content of a folder of a single file to a destination.
   *
   * @param source      the path to the source folder or file
   * @param destination the path to the destination folder or file
   * @param overwrite   If true, the destination path will be overwritten.
   *                    If false and the destination is not empty then the method will fail.
   */
  override def move(source: String, destination: String, overwrite: Boolean): Unit = {
    copy(source, destination,overwrite)
    remove(source)
  }

  /**
   * Permanently delete a folder or a file from the system.
   *
   * @param path path to delete from the system.
   */
override def remove(path: String): Unit = {
  val pathUri = new URI(path)
try {
  if (isDirectory(path)) {
    val prefix = normalizeToDirectoryPrefix(pathUri)
    var listObjectsV2Response : ListObjectsV2Response = null
    val listObjectsV2RequestBuilder = ListObjectsV2Request.builder.bucket(pathUri.getHost)

    if (prefix == DELIMITER) {
      val listObjectsV2Request = listObjectsV2RequestBuilder.build
      listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request)
    }
    else {
      val listObjectsV2Request = listObjectsV2RequestBuilder.prefix(prefix).build
      listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request)
    }

    listObjectsV2Response.contents.stream.forEach{ element =>
      val deleteObjectRequest = DeleteObjectRequest.builder.bucket(pathUri.getHost).key(element.key).build
      s3Client.deleteObject(deleteObjectRequest)
    }

  } else {
    val prefix = sanitizePath(pathUri.getPath)
    val deleteObjectRequest = DeleteObjectRequest.builder.bucket(pathUri.getHost).key(prefix).build
    s3Client.deleteObject(deleteObjectRequest)
  }
} catch {
    case _: NoSuchKeyException =>
      log.error("Path does not exit!")
  }
}
}

