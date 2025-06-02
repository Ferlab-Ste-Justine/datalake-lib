package bio.ferlab.datalake.testutils

import org.apache.logging.log4j.core.{LoggerContext, LogEvent}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.Level
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer

trait WithLogCapture {

  def withLogCapture[T](loggerName: String)(block:  ListAppender => T): T = {
    val appender = new ListAppender("List");
    appender.start();

    val loggerConfig = LoggerContext.getContext(false).getConfiguration().getLoggerConfig(loggerName)
    loggerConfig.addAppender(appender, Level.ALL, null)

    try { block(appender) }
    finally {
      loggerConfig.removeAppender("List")
      appender.stop()
    }
  }

  def withLogCapture[T](clazz: Class[_])(block: ListAppender => T): T = withLogCapture(clazz.getCanonicalName)(block)
}

case class ListAppender(name: String) extends AbstractAppender(name, null, null, false, null) {

  private val events = ListBuffer.empty[LogEvent]

  def append(event: LogEvent): Unit = {
    events += event
  }

  def getEvents: Seq[LogEvent] = events.toSeq

  override def stop(): Unit = {
    super.stop()
    events.clear()
  }
}