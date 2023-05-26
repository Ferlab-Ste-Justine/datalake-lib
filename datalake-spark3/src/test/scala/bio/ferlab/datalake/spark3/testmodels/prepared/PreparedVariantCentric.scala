package bio.ferlab.datalake.spark3.testmodels.prepared

import PreparedVariantCentric._
import bio.ferlab.datalake.spark3.testmodels.enriched.{CONSERVATIONS, PREDICTIONS}
import bio.ferlab.datalake.spark3.testmodels.prepared.PreparedVariantCentric.GENES.noGene

case class PreparedVariantCentric(`chromosome`: String = "1",
                                  `start`: Long = 69897,
                                  `reference`: String = "T",
                                  `alternate`: String = "C",
                                  `end`: Long = 69898,
                                  `locus`: String = "1-69897-T-C",
                                  `hash`: String = "314c8a3ce0334eab1a9358bcaf8c6f4206971d92",
                                  `hgvsg`: String = "chr1:g.69897T>C",
                                  `variant_class`: String = "SNV",
                                  `assembly_version`: String = "GRCh38",
                                  `frequency`: FREQUENCY = FREQUENCY(),
                                  `external_frequencies`: EXTERNAL_FREQUENCIES = EXTERNAL_FREQUENCIES(),
                                  `clinvar`: CLINVAR = CLINVAR(),
                                  `rsnumber`: String = "rs200676709",
                                  `dna_change`: String = "T>C",
                                  `genes`: Set[GENES] = Set(
                                    GENES(),
                                    GENES(`symbol` = "gene2", `consequences` = Seq(CONSEQUENCES(`ensembl_transcript_id` = "transcript3"), CONSEQUENCES(`ensembl_transcript_id` = "transcript4"))),
                                    noGene(Seq(CONSEQUENCES(`ensembl_transcript_id` = "transcript2")))
                                  ),
                                  `variant_external_reference`: Seq[String] = Seq("DBSNP", "Clinvar"),
                                  `gene_external_reference`: Seq[String] = Seq("HPO", "Orphanet", "OMIM"))

object PreparedVariantCentric {
  case class THOUSAND_GENOMES(`ac`: Long = 3446,
                              `an`: Long = 5008,
                              `af`: Double = 0.688099)

  case class ORPHANET(`disorder_id`: Long = 17827,
                      `panel`: String = "Immunodeficiency due to a classical component pathway complement deficiency",
                      `inheritance`: Seq[String] = Seq("Autosomal recessive"))

  case class EXTERNAL_FREQUENCIES(`thousand_genomes`: THOUSAND_GENOMES = THOUSAND_GENOMES(),
                                  `topmed_bravo`: TOPMED_BRAVO = TOPMED_BRAVO(),
                                  `gnomad_genomes_2_1_1`: GNOMAD_GENOMES_2_1_1 = GNOMAD_GENOMES_2_1_1(),
                                  `gnomad_exomes_2_1_1`: GNOMAD_EXOMES_2_1_1 = GNOMAD_EXOMES_2_1_1(),
                                  `gnomad_genomes_3`: GNOMAD_GENOMES_3 = GNOMAD_GENOMES_3())

  case class DDD(`disease_name`: String = "OCULOAURICULAR SYNDROME")

  case class GNOMAD(`pli`: Float = 1.0f,
                    `loeuf`: Float = 0.054f)

  case class GNOMAD_GENOMES_2_1_1(`ac`: Long = 1,
                                  `an`: Long = 26342,
                                  `af`: Double = 3.7962189659099535E-5,
                                  `hom`: Long = 0)

  case class FREQUENCY(`total`: TOTAL = TOTAL())

  case class CLINVAR(`clinvar_id`: String = "257668",
                     `clin_sig`: Seq[String] = Seq("Benign"),
                     `conditions`: Seq[String] = Seq("Congenital myasthenic syndrome 12", "not specified", "not provided"),
                     `inheritance`: Seq[String] = Seq("germline"),
                     `interpretations`: Seq[String] = Seq("Benign"))

  case class AMINO_ACIDS(`reference`: String = "S",
                         `variant`: Option[String] = None)

  case class CONSEQUENCES(`ensembl_transcript_id`: String = "ENST00000335137",
                          `consequences`: Seq[String] = Seq("synonymous_variant"),
                          `vep_impact`: String = "LOW",
                          `ensembl_feature_id`: String = "ENST00000335137",
                          `feature_type`: String = "Transcript",
                          `strand`: Int = 1,
                          `exon`: EXON = EXON(),
                          `intron`: INTRON = INTRON(),
                          `hgvsc`: String = "ENST00000335137.4:c.807T>C",
                          `hgvsp`: String = "ENSP00000334393.3:p.Ser269=",
                          `cds_position`: Int = 807,
                          `cdna_position`: Int = 843,
                          `protein_position`: Int = 269,
                          `amino_acids`: AMINO_ACIDS = AMINO_ACIDS(),
                          `codons`: CODONS = CODONS(),
                          `refseq_mrna_id`: Seq[String] = Seq("NM_001005484.1", "NM_001005484.2"),
                          `aa_change`: String = "p.Ser269=",
                          `coding_dna_change`: String = "c.807T>C",
                          `impact_score`: Int = 2,
                          `consequence`: Seq[String] = Seq("synonymous"),
                          `predictions`: PREDICTIONS = PREDICTIONS(),
                          `conservations`: CONSERVATIONS = CONSERVATIONS(),
                          `uniprot_id`: Option[String] = None,
                          `mane_select`: Boolean = false,
                          `mane_plus`: Boolean = false,
                          `canonical`: Boolean = false,
                          `picked`: Boolean = true)

  case class GENES(`symbol`: String = "OR4F5",
                   `entrez_gene_id`: Option[String] = Some("777"),
                   `omim_gene_id`: Option[String] = Some("601013"),
                   `hgnc`: Option[String] = Some("HGNC:1392"),
                   `ensembl_gene_id`: Option[String] = Some("ENSG00000198216"),
                   `location`: Option[String] = Some("1q25.3"),
                   `name`: Option[String] = Some("calcium voltage-gated channel subunit alpha1 E"),
                   `alias`: Option[Seq[String]] = Some(Seq("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139")),
                   `biotype`: Option[String] = Some("protein_coding"),
                   `orphanet`: Option[Seq[ORPHANET]] = Some(Seq(ORPHANET())),
                   `hpo`: Option[Seq[HPO]] = Some(Seq(HPO())),
                   `omim`: Option[Seq[OMIM]] = Some(Seq(OMIM())),
                   `ddd`: Option[Seq[DDD]] = Some(Seq(DDD())),
                   `cosmic`: Option[Seq[COSMIC]] = Some(Seq(COSMIC())),
                   `spliceai`: Option[SPLICEAI] = Some(SPLICEAI()),
                   `gnomad`: Option[GNOMAD] = Some(GNOMAD()),
                   `consequences`: Seq[CONSEQUENCES] = Seq(CONSEQUENCES())
                  )


  case class GNOMAD_GENOMES_3(`ac`: Long = 10,
                              `an`: Long = 20,
                              `af`: Double = 0.5,
                              `hom`: Long = 10)

  case class COSMIC(`tumour_types_germline`: Seq[String] = Seq("breast", "colon", "endometrial cancer under age 50"))

  case class EXON(`rank`: Int = 1,
                  `total`: Int = 1)

  case class OMIM(`name`: String = "Epileptic encephalopathy, early infantile, 69",
                  `omim_id`: String = "618285",
                  `inheritance`: Seq[String] = Seq("Autosomal dominant"),
                  `inheritance_code`: Seq[String] = Seq("AD"))

  case class TOTAL(`ac`: Long = 4,
                   `an`: Long = 6,
                   `pc`: Long = 2,
                   `pn`: Long = 3,
                   `hom`: Long = 2)

  case class HPO(`hpo_term_id`: String = "HP:0001347",
                 `hpo_term_name`: String = "Hyperreflexia",
                 `hpo_term_label`: String = "Hyperreflexia (HP:0001347)")

  case class SPLICEAI(`ds`: Double = 0.01,
                      `type`: Seq[String] = Seq("AG"))

  case class TOPMED_BRAVO(`ac`: Long = 2,
                          `an`: Long = 125568,
                          `af`: Double = 1.59276E-5,
                          `hom`: Long = 0,
                          `het`: Long = 2)

  case class GNOMAD_EXOMES_2_1_1(`ac`: Long = 0,
                                 `an`: Long = 2,
                                 `af`: Double = 0.0,
                                 `hom`: Long = 0)

  case class INTRON(`rank`: Option[Int] = None,
                    `total`: Option[Int] = None)

  case class CODONS(`reference`: String = "tcT",
                    `variant`: String = "tcC")

  object GENES {

    def noGene(consequences: Seq[CONSEQUENCES]): GENES = GENES(`symbol` = "NO_GENE", `entrez_gene_id` = None, `omim_gene_id` = None, `hgnc` = None, `ensembl_gene_id` = None, `location` = None, `name` = None, `alias` = None, `biotype` = None, `orphanet` = None, hpo = None, `omim` = None, ddd = None, `cosmic` = None, `spliceai` = None, `gnomad` = None, `consequences` = consequences)
  }
}