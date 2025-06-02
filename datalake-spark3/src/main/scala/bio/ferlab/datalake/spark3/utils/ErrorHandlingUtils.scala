package bio.ferlab.datalake.spark3.utils

import org.slf4j
import scala.util.Try

object ErrorHandlingUtils {

  implicit val log: slf4j.Logger = slf4j.LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * Throw an error if the ETL run results contain a [[Throwable]]
   *
   * @param runs Iterable of ETL run results
   */
  def runOptThrow(runs: => Iterable[Option[Throwable]]): Unit = {
    runs.flatten.headOption.foreach(t => throw t)
  }

  /**
   * Try a function and optionally return a [[Throwable]]
   *
   * @param f            function to try
   * @param errorMessage message to log if the try fails
   * @tparam T return type of the function f
   * @return a throwable if the function failed
   */
  def optIfFailed[T](f: => T, errorMessage: => String): Option[Throwable] =
    Try(f).failed.map { t =>
      log.error(errorMessage, t)
      t
    }.toOption
}
