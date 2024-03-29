/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2023-03-10T15:22:17.505806
 */
package bio.ferlab.datalake.testutils.models.enriched

import bio.ferlab.datalake.testutils.models.normalized.{AminoAcids, CODONS, Exon, Intron}

import java.sql.Timestamp


case class EnrichedConsequences(`chromosome`: String = "1",
                                `start`: Long = 69897,
                                `reference`: String = "T",
                                `alternate`: String = "C",
                                `ensembl_transcript_id`: String = "ENST00000335137",
                                `ensembl_gene_id`: String = "ENSG00000186092",
                                `vep_impact`: String = "LOW",
                                `symbol`: String = "OR4F5",
                                `ensembl_feature_id`: String = "ENST00000335137",
                                `feature_type`: String = "Transcript",
                                `strand`: Int = 1,
                                `biotype`: String = "protein_coding",
                                `exon`: Exon = Exon(),
                                `intron`: Intron = Intron(),
                                `hgvsc`: String = "ENST00000335137.4:c.807T>C",
                                `hgvsp`: String = "ENSP00000334393.3:p.Ser269=",
                                `cds_position`: Int = 807,
                                `cdna_position`: Int = 843,
                                `protein_position`: Int = 269,
                                `amino_acids`: AminoAcids = AminoAcids(),
                                `codons`: CODONS = CODONS(),
                                `original_canonical`: Boolean = true,
                                `refseq_mrna_id`: Seq[String] = Seq("NM_001005484.1", "NM_001005484.2"),
                                `refseq_protein_id`: Option[String] = Some("NP_001005277"),
                                `aa_change`: String = "p.Ser269=",
                                `coding_dna_change`: String = "c.807T>C",
                                `impact_score`: Int = 2,
                                `created_on`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:41:31.039545"),
                                `updated_on`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:41:31.039545"),
                                `normalized_consequences_oid`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:41:31.039545"),
                                `consequence`: Seq[String] = Seq("synonymous"),
                                `predictions`: PREDICTIONS = PREDICTIONS(),
                                `conservations`: CONSERVATIONS = CONSERVATIONS(),
                                `consequences_oid`: Timestamp = java.sql.Timestamp.valueOf("2022-04-06 13:41:31.039545"),
                                `uniprot_id`: Option[String] = Some("Q6IEY1"),
                                `mane_select`: Boolean = true,
                                `mane_plus`: Boolean = true,
                                `canonical`: Boolean = true,
                                `picked`: Option[Boolean] = Some(true))

case class PREDICTIONS(`sift_score`: Option[Double] = None,
                       `sift_pred`: Option[String] = None,
                       `polyphen2_hvar_score`: Option[Double] = None,
                       `polyphen2_hvar_pred`: Option[String] = None,
                       `fathmm_score`: Option[Double] = None,
                       `fathmm_pred`: Option[String] = None,
                       `cadd_score`: Option[Double] = None,
                       `cadd_phred`: Option[Double] = None,
                       `dann_score`: Option[Double] = None,
                       `revel_score`: Option[Double] = None,
                       `lrt_score`: Option[Double] = None,
                       `lrt_pred`: Option[String] = None)

case class CONSERVATIONS(`phyloP17way_primate`: Option[Double] = None, phyloP100way_vertebrate: Option[Double] = None)
