package bio.ferlab.datalake.commons.config

import bio.ferlab.datalake.commons.config.testutils.ExtraConf
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.generic.auto._
class ConfigurationLoaderSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  "loadEtlConfiguration" should "parse a valid config file into a Configuration object" in {

    val parsedConf = ConfigurationLoader.loadFromPath[SimpleConfiguration](getClass.getClassLoader.getResource("config/application.conf").getFile)

    parsedConf shouldBe SimpleConfiguration(DatalakeConf(storages = List(StorageConf("default", "spark-local", S3))))
  }

  "loadEtlConfiguration" should "not throw an exception but use default config when config file not found" in {

    val parsedConf = ConfigurationLoader.loadFromPath[SimpleConfiguration]("config/application.conf")

    parsedConf shouldBe SimpleConfiguration(DatalakeConf(storages = List(StorageConf("default", "spark-fallback", S3))))
  }

  "loadEtlConfiguration" should "load config from resources" in {

    val parsedConf = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/application.conf")

    parsedConf shouldBe SimpleConfiguration(DatalakeConf(storages = List(StorageConf("default", "spark-local", S3))))
  }

  "loadEtlConfiguration" should "load config from string" in {

    val conf =
      """
        |datalake = {
        |   storages = [
        |     {
        |      id = "default"
        |      path = "spark-local"
        |      filesystem = "S3"
        |     }
        |   ]
        |}
        |""".stripMargin

    val parsedConf = ConfigurationLoader.loadFromString[SimpleConfiguration](conf)

    parsedConf shouldBe SimpleConfiguration(DatalakeConf(storages = List(StorageConf("default", "spark-local", S3))))
  }

  it should "load config for an class that extends Configuration" in {
    val conf =
      """
        |extraOption = "hello"
        |datalake = {
        |   storages = [
        |     {
        |      id = "default"
        |      path = "spark-local"
        |      filesystem = "S3"
        |     }
        |   ]
        |}
        |""".stripMargin

    val parsedConf = ConfigurationLoader.loadFromString[ExtraConf](conf)

    parsedConf shouldBe ExtraConf("hello", DatalakeConf(storages = List(StorageConf("default", "spark-local", S3))))

  }

  it should "load config with env variable" in {

    val p = this.getClass.getClassLoader.getResource("config/env.conf").getFile
    val URL = sys.env.get("URL")
    val ENV_URL = ConfigurationLoader.loadFromPath[SimpleConfiguration](p).storages.head.path

    URL.getOrElse("fallback_url") shouldBe ENV_URL

  }

}
