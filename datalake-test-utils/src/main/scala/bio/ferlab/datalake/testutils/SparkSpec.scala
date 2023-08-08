package bio.ferlab.datalake.testutils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

trait SparkSpec extends AnyFlatSpec with WithSparkSession with Matchers
