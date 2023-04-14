package bio.ferlab.datalake.commons.config

import pureconfig.ConfigConvert

sealed trait RunStep {
  val order: Int
}

/**
 * List of all RunStep supported by an [[ETL]]
 */
object RunStep {
  case object sample    extends RunStep { override val order = 0 }
  case object reset     extends RunStep { override val order = 1 }
  case object extract   extends RunStep { override val order = 2 }
  case object transform extends RunStep { override val order = 3 }
  case object load      extends RunStep { override val order = 4 }
  case object publish   extends RunStep { override val order = 5 }

  final val allSteps = Seq(reset, sample, extract, transform, load, publish)
  final val default_load = Seq(extract, transform, load, publish)
  final val initial_load = Seq(reset, extract, transform, load, publish)

  def fromString(str: String): RunStep = {
    str match {
      case "reset" => reset
      case "sample" => sample
      case "extract" => extract
      case "transform" => transform
      case "load" => load
      case "publish" => publish
      case _ => throw new IllegalArgumentException(s"RunStep $str unknown")
    }
  }

  /**
   * Parse steps passed in argument.
   * {{{
   *   'skip' will be parse as a empty list of steps to run
   *   'sample' will effectively run all steps including the sampling steps
   *   'default' will be parsed as Seq(extract, transform, load, publish)
   *   'initial' will be parsed as Seq(reset, extract, transform, load, publish)
   * }}}
   *
   * It is also possible to use custom steps and pass the first and last step as argument. For instance:
   * {{{
   *   'extract_load' will run all the steps between extract and load and skip 'published'
   * }}}
   * @param str steps as a string
   * @return
   */
  def getSteps(str: String): Seq[RunStep] = {
    str.split("_").toList match {
      case Nil => allSteps
      case List("skip") => Seq()
      case List("sample") => allSteps
      case List("default") => default_load
      case List("initial") => initial_load
      case head :: Nil => List(fromString(head))
      case first_step :: last_step :: Nil =>
        val fs = fromString(first_step)
        val ls = fromString(last_step)
        allSteps.filter(s => s.order >= fs.order && s.order <= ls.order)
      case steps =>
        steps.map(fromString)
    }
  }


  implicit val converter: ConfigConvert[RunStep] = enumConvert[RunStep]
}
