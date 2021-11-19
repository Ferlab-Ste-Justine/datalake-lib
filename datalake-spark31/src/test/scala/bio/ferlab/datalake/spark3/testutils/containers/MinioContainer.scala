package bio.ferlab.datalake.spark3.testutils

import bio.ferlab.datalake.spark3.testutils.containers.OurContainer
import com.dimafeng.testcontainers.GenericContainer

case object MinioContainer extends OurContainer {
  val name = "datalake-minio-test"
  val port = 9000
  val accessKey = "accesskey"
  val secretKey = "secretkey"
  val region = "region"
  val container: GenericContainer = GenericContainer(
    "minio/minio",
    command = Seq("server", "/data"),
    exposedPorts = Seq(port),
    labels = Map("name" -> name),
    env = Map("MINIO_ACCESS_KEY" -> accessKey, "MINIO_SECRET_KEY" -> secretKey)
  )


}