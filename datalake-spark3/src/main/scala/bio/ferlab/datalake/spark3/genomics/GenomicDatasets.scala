package bio.ferlab.datalake.spark3.genomics

import bio.ferlab.datalake.commons.config.Format.DELTA
import bio.ferlab.datalake.commons.config.LoadType.Scd1
import bio.ferlab.datalake.commons.config.{BaseDatasets, DatasetConf}

case class GenomicDatasets(alias: String, tableDatabase: Option[String], viewDatabase: Option[String]) extends BaseDatasets {
  override val sources: List[DatasetConf] = List(
    DatasetConf("normalized_consequences", alias, "/normalized/consequences", DELTA, Scd1, partitionby = List("chromosome"), table = table("normalized_consequences"), view = view("normalized_consequences"), keys = List("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")),
    DatasetConf("enriched_consequences"  , alias, "/enriched/consequences"  , DELTA, Scd1, partitionby = List("chromosome"), table = table("consequences")           , view = view("consequences")           , keys = List("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")),
  )
}
