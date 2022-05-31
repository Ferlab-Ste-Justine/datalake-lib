package bio.ferlab.datalake.spark3.testutils.containers

import com.dimafeng.testcontainers.GenericContainer

import scala.collection.JavaConverters._

trait OurContainer {
  def container: GenericContainer

  private var isStarted = false

  def name: String

  def port: Int

  private var publicPort: Int = -1

  def startIfNotRunning(): Int = {
    if (isStarted) {
      publicPort
    } else {
      val runningContainer = container.dockerClient.listContainersCmd().withLabelFilter(Map("name" -> name).asJava).exec().asScala

      runningContainer.toList match {
        case Nil =>
          container.start()
          publicPort = container.mappedPort(port)
        case List(c) =>
          publicPort = c.ports.collectFirst { case p if p.getPrivatePort == port => p.getPublicPort }.get
      }
      isStarted = true
      publicPort
    }
  }


}
