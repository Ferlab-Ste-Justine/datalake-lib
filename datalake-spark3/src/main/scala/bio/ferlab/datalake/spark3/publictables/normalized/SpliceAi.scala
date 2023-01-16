package bio.ferlab.datalake.spark3.publictables.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits.DatasetConfOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import bio.ferlab.datalake.spark3.utils.RepartitionByRange
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class SpliceAi(variantType: String)(implicit conf: Configuration) extends ETLP {
  override val mainDestination: DatasetConf = conf.getDataset(s"normalized_spliceai_$variantType")
  val raw_spliceai: DatasetConf = conf.getDataset(s"raw_spliceai_$variantType")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now())
                      (implicit spark: SparkSession): Map[String, DataFrame] = {
    Map(
      raw_spliceai.id -> raw_spliceai.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now())
                              (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val df = data(raw_spliceai.id)

    df
      .select(
        chromosome +:
          start +:
          end +:
          reference +:
          alternate +:
          flattenInfo(df, except = "INFO_OLD_MULTIALLELIC", "INFO_FILTERS"): _*
      )
      .withColumn("spliceai", split($"spliceai", "\\|"))
      .withColumn("allele", $"spliceai"(0))
      .withColumn("symbol", $"spliceai"(1))
      .withColumn("ds_ag", $"spliceai"(2).cast("double"))
      .withColumn("ds_al", $"spliceai"(3).cast("double"))
      .withColumn("ds_dg", $"spliceai"(4).cast("double"))
      .withColumn("ds_dl", $"spliceai"(5).cast("double"))
      .withColumn("dp_ag", $"spliceai"(6).cast("int"))
      .withColumn("dp_al", $"spliceai"(7).cast("int"))
      .withColumn("dp_dg", $"spliceai"(8).cast("int"))
      .withColumn("dp_dl", $"spliceai"(9).cast("int"))
      .drop("spliceai")
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(50))

}
