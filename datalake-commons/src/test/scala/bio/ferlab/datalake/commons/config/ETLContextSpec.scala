package bio.ferlab.datalake.commons.config

import bio.ferlab.datalake.commons.config.RunStep._
import mainargs.{ParserForMethods, main}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ETLContextSpec extends AnyFlatSpec with Matchers {

  it should "parse steps into a Seq of RunStep" in {
    object Main {
      @main
      def run(rc: RuntimeETLContext): RuntimeETLContext = rc

      def main(args: Array[String]): Seq[RunStep] = ParserForMethods(this).runOrExit(args)
        .leftSide
        .asInstanceOf[RuntimeETLContext]
        .steps
    }

    val result: Seq[RunStep] = Main.main(Array("--config", "", "--steps", "default", "--app-name", "test"))
    result should contain theSameElementsAs default_load
  }

}
