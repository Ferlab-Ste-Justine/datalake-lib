package ca.ferlab.datalake.core.config

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigurationLoaderSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  "loadEtlConfiguration" should "parse a valid config file into a Configuration object" in {

    val parsedConf = ConfigurationLoader.loadEtlConfiguration(getClass.getClassLoader.getResource("config/application.conf").getFile)

    parsedConf shouldBe Configuration(List(StorageConf("default", "spark-local")))
  }

  "loadEtlConfiguration" should "not throw an exception but use default config when config file not found" in {

    val parsedConf = ConfigurationLoader.loadEtlConfiguration("config/application.conf")

    parsedConf shouldBe Configuration(List(StorageConf("default", "spark-fallback")))
  }

}
