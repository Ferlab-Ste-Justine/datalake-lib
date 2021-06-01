package bio.ferlab.datalake.spark3.hive
import io.projectglow.sql.util.SerializableConfiguration
import io.projectglow.vcf.VCFFileFormat
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.collection.JavaConverters._

object VcfHeaderParser {

  case class Header(id: String, key: String, `type`: String, description: String) {
    def toHiveFieldComment: HiveFieldComment = {
      HiveFieldComment(id, `type`, description)
    }
  }

  def getVcfHeaders(vcfFilePath: String)(implicit spark: SparkSession): List[Header] = {

    val serializableConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    val (header, _) = VCFFileFormat.createVCFCodec(vcfFilePath, serializableConf.value)

    header
      .getInfoHeaderLines
      .asScala
      .toList
      .map(h => Header(h.getID, h.getID, h.getType.name(), h.getDescription))
  }

  def writeDocumentationFileAsJson(inputPath: String, outputPath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val headers = getVcfHeaders(inputPath)
      .map(_.toHiveFieldComment)

    val jsonHeaders = write(headers)(DefaultFormats)
    Seq(jsonHeaders)
      .toDF()
      .coalesce(1)
      .write
      .mode("overwrite")
      .text(outputPath)
  }

}
