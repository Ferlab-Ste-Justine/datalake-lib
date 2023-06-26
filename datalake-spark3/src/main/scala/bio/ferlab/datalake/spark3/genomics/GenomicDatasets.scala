package bio.ferlab.datalake.spark3.genomics

import bio.ferlab.datalake.commons.config.Format.{DELTA, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.{OverWrite, Scd1}
import bio.ferlab.datalake.commons.config.{BaseDatasets, DatasetConf, FixedRepartition, RepartitionByColumns, RepartitionByRange}

case class GenomicDatasets(alias: String, tableDatabase: Option[String], viewDatabase: Option[String]) extends BaseDatasets {
  override val sources: List[DatasetConf] = List(
    DatasetConf("normalized_consequences", alias, "/normalized/consequences", DELTA, Scd1, partitionby = List("chromosome"), table = table("normalized_consequences"), view = view("normalized_consequences"), keys = List("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")),
    DatasetConf("enriched_consequences", alias, "/enriched/consequences", DELTA, Scd1, partitionby = List("chromosome"), table = table("consequences"), view = view("consequences"), keys = List("chromosome", "start", "reference", "alternate", "ensembl_transcript_id"), repartition = Some(RepartitionByRange(Seq("chromosome", "start"), n = Some(100)))),
    DatasetConf("enriched_variants", alias, "/enriched/variants", DELTA, OverWrite, partitionby = List("chromosome"), table = table("variants"), view = view("variants"), repartition = Some(RepartitionByRange(Seq("chromosome", "start"), n = Some(100)))),
    DatasetConf("es_index_variant_centric", alias, "/es_index/variant_centric", PARQUET, OverWrite, partitionby = List("chromosome"), table = table("variant_centric"), view = view("variant_centric"), repartition = Some(RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(100)))),
    DatasetConf("es_index_variant_suggestions", alias, "/es_index/variant_suggestions", PARQUET, OverWrite, partitionby = List("chromosome"), table = table("variant_suggestions"), view = view("variant_suggestions"), repartition = Some(RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(100)))),
    DatasetConf("es_index_gene_centric", alias, "/es_index/gene_centric", PARQUET, OverWrite, partitionby = List("chromosome"), table = table("gene_centric"), view = view("gene_centric"), repartition = Some(RepartitionByColumns(columnNames = Seq("chromosome"), n = Some(100)))),
    DatasetConf("es_index_gene_suggestions", alias, "/es_index/gene_suggestions", PARQUET, OverWrite, partitionby = List("chromosome"), table = table("gene_suggestions"), view = view("gene_suggestions"), repartition = Some(FixedRepartition(20)))
  )
}
