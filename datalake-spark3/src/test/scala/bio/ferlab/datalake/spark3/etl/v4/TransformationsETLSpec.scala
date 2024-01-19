package bio.ferlab.datalake.spark3.etl.v4

import bio.ferlab.datalake.spark3.etl.AirportOutput
import bio.ferlab.datalake.spark3.testutils.AirportInput
import bio.ferlab.datalake.spark3.transformation._
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.functions.col

import java.time.LocalDateTime

class TransformationsETLSpec extends WithETL {

  import spark.implicits._

  override val defaultJob = new TransformationsETL(TestETLContext(), srcConf, destConf,
    List(
      DuplicateColumn("id", "hash_id") -> SHA1("", "hash_id"),
      ToLong("id"),
      Trim("CODE", "description"),
      InputFileName("input_file_name"),
      CurrentTimestamp("createdOn"),
      Rename(Map(
        "id" -> "airport_id",
        "CODE" -> "airport_cd",
        "description" -> "description_EN"
      ))
    ),
  )

  type T = LocalDateTime

  "extract" should "return the expected format" in {

    val data = defaultJob.extract()
    data(srcConf.id).as[AirportInput]
  }

  "transform" should "return the expected format" in {

    val input = defaultJob.extract()
    val output = defaultJob.transformSingle(input, LocalDateTime.now(), LocalDateTime.now())
    val head = output.as[AirportOutput].where(col("airport_id") === 1).collect().head
    head shouldBe AirportOutput(input_file_name = head.input_file_name, createdOn = head.createdOn)

  }

  "load" should "create the expected table" in {

    val output = Seq(AirportOutput()).toDF()

    defaultJob.loadSingle(output)

    val table = spark.table(s"${destConf.table.get.fullName}")
    table.as[AirportOutput].collect().head shouldBe AirportOutput()
  }

}
