package bio.ferlab.datalake.commons.config

import bio.ferlab.datalake.commons.config.Format.PARQUET
import bio.ferlab.datalake.commons.config.LoadType._
import bio.ferlab.datalake.commons.config.testutils.ExtraConf
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.enum._

class ConfigurationWriterSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  val conf: SimpleConfiguration = SimpleConfiguration(
    DatalakeConf(
      args = List("arg1", "arg2"),
      storages = List(StorageConf("a", "s3://a", S3)),
      sources = List(
        DatasetConf("name_a", "a", "/path/a", PARQUET, OverWrite, Some(TableConf("db", "name_a")), List("id"), List(), Map("key" -> "value"), Map("key2" -> "value")),
        DatasetConf("name_b", "b", "/path/b", PARQUET, OverWrite, Some(TableConf("db", "name_b")), List("id"), List(), Map("key" -> "value"), Map("key2" -> "value"))),
      sparkconf = Map("spark.conf1" -> "v1", "spark.conf2" -> "${?v2}")
    )
  )

  "ConfigurationWriter" should "read a conf and convert it to readable hocon configuration" in {

    println(ConfigurationWriter.toHocon(conf))
    ConfigurationWriter.toHocon(conf) shouldBe
      """|datalake {
         |    args=[
         |        arg1,
         |        arg2
         |    ]
         |    sources=[
         |        {
         |            format=PARQUET
         |            id="name_a"
         |            keys=[
         |                id
         |            ]
         |            loadtype=OverWrite
         |            partitionby=[]
         |            path="/path/a"
         |            readoptions {
         |                key=value
         |            }
         |            storageid=a
         |            table {
         |                database=db
         |                name="name_a"
         |            }
         |            writeoptions {
         |                key2=value
         |            }
         |        },
         |        {
         |            format=PARQUET
         |            id="name_b"
         |            keys=[
         |                id
         |            ]
         |            loadtype=OverWrite
         |            partitionby=[]
         |            path="/path/b"
         |            readoptions {
         |                key=value
         |            }
         |            storageid=b
         |            table {
         |                database=db
         |                name="name_b"
         |            }
         |            writeoptions {
         |                key2=value
         |            }
         |        }
         |    ]
         |    sparkconf {
         |        "spark.conf1"=v1
         |        "spark.conf2"="${?v2}"
         |    }
         |    storages=[
         |        {
         |            filesystem=S3
         |            id=a
         |            path="s3://a"
         |        }
         |    ]
         |}
         |""".stripMargin
  }


  it should "write a file in hocon format" in {
    val path = getClass.getClassLoader.getResource(".").getFile + "example.conf"

    ConfigurationWriter.writeTo(path, conf)

    val file = new java.io.File(path)
    file.exists() shouldBe true

    val writtenConf: Configuration = ConfigSource.file(path).loadOrThrow[SimpleConfiguration]
    writtenConf shouldBe conf.copy(conf.datalake.copy(sparkconf = Map("spark.conf1" -> "v1"))) // Since v2 env variable does not exist.
  }

  it should "write configuration for inherited class" in {
    val extraConf = ExtraConf(extraOption = "hello", datalake = conf.datalake)
    println(ConfigurationWriter.toHocon(extraConf))
    ConfigurationWriter.toHocon(extraConf) shouldBe
      """|datalake {
         |    args=[
         |        arg1,
         |        arg2
         |    ]
         |    sources=[
         |        {
         |            format=PARQUET
         |            id="name_a"
         |            keys=[
         |                id
         |            ]
         |            loadtype=OverWrite
         |            partitionby=[]
         |            path="/path/a"
         |            readoptions {
         |                key=value
         |            }
         |            storageid=a
         |            table {
         |                database=db
         |                name="name_a"
         |            }
         |            writeoptions {
         |                key2=value
         |            }
         |        },
         |        {
         |            format=PARQUET
         |            id="name_b"
         |            keys=[
         |                id
         |            ]
         |            loadtype=OverWrite
         |            partitionby=[]
         |            path="/path/b"
         |            readoptions {
         |                key=value
         |            }
         |            storageid=b
         |            table {
         |                database=db
         |                name="name_b"
         |            }
         |            writeoptions {
         |                key2=value
         |            }
         |        }
         |    ]
         |    sparkconf {
         |        "spark.conf1"=v1
         |        "spark.conf2"="${?v2}"
         |    }
         |    storages=[
         |        {
         |            filesystem=S3
         |            id=a
         |            path="s3://a"
         |        }
         |    ]
         |}
         |extraOption=hello
         |""".stripMargin

  }

}