package bio.ferlab.datalake.spark3.genomics.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SimpleETLP
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import java.sql.Timestamp
import java.time.LocalDateTime

abstract class BaseConsequences(rc: RuntimeETLContext, annotationsColumn: Column = csq, groupByLocus: Boolean = true) extends SimpleETLP(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_consequences")

  val raw_vcf: String = "raw_vcf"

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    val groupedByLocus = if (groupByLocus) {
      data(raw_vcf)
        .select(
          chromosome,
          start,
          end,
          reference,
          alternate,
          name,
          annotationsColumn
        )
        .groupBy(locus: _*)
        .agg(
          first("annotations") as "annotations",
          first("name") as "name",
          first("end") as "end"
        )
    } else {
      data(raw_vcf).select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        annotationsColumn
      )
    }
    val explodedAnnotations = groupedByLocus.withColumn("annotation", explode($"annotations"))
      .drop("annotations")
    explodedAnnotations.select($"*",
      consequences,
      impact,
      symbol,
      ensembl_gene_id,
      ensembl_feature_id,
      ensembl_transcript_id,
      ensembl_regulatory_id,
      feature_type,
      strand,
      biotype,
      variant_class,
      exon,
      intron,
      hgvsc,
      regexp_replace(hgvsp, "%3D", "=") as "hgvsp",
      hgvsg,
      cds_position,
      cdna_position,
      protein_position,
      amino_acids,
      codons,
      original_canonical
    )
      .withRefseqMrnaId()
      .drop("annotation")
      .withColumn("aa_change",
        concat(
          lit("p."),
          normalizeAminoAcid($"amino_acids.reference"),
          $"protein_position",
          when($"amino_acids.variant".isNull, lit("=")).otherwise(normalizeAminoAcid($"amino_acids.variant"))))
      .withColumn("coding_dna_change", when($"cds_position".isNotNull, concat(lit("c."), $"cds_position", $"reference", lit(">"), $"alternate")).otherwise(lit(null)))
      .withColumn("impact_score", when($"impact" === "MODIFIER", 1).when($"impact" === "LOW", 2).when($"impact" === "MODERATE", 3).when($"impact" === "HIGH", 4).otherwise(0))
      .withColumn("created_on", lit(Timestamp.valueOf(currentRunDateTime)))
      .withColumn("updated_on", lit(Timestamp.valueOf(currentRunDateTime)))
      .withColumn(mainDestination.oid, col("created_on"))
      .dropDuplicates("chromosome", "start", "reference", "alternate", "ensembl_transcript_id")

  }


  private def normalizeAminoAcid(amino_acid: Column): Column = {
    val aminoAcidMap =
      Map(
        "A" -> "Ala",
        "R" -> "Arg",
        "N" -> "Asn",
        "D" -> "Asp",
        "B" -> "Asx",
        "C" -> "Cys",
        "E" -> "Glu",
        "Q" -> "Gln",
        "Z" -> "Glx",
        "G" -> "Gly",
        "H" -> "His",
        "I" -> "Ile",
        "L" -> "Leu",
        "K" -> "Lys",
        "M" -> "Met",
        "F" -> "Phe",
        "P" -> "Pro",
        "S" -> "Ser",
        "T" -> "Thr",
        "W" -> "Trp",
        "Y" -> "Tyr",
        "V" -> "Val",
        "X" -> "Xaa",
        "*" -> "Ter"
      )
    aminoAcidMap
      .tail
      .foldLeft(when(amino_acid === aminoAcidMap.head._1, lit(aminoAcidMap.head._2))) { case (c, (a, aaa)) =>
        c.when(amino_acid === a, lit(aaa))
      }.otherwise(amino_acid)
  }


}
