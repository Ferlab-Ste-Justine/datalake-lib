/**
 * Generated by [[bio.ferlab.datalake.testutils.ClassGenerator]]
 * on 2023-08-10T16:50:15.592397
 */
package bio.ferlab.datalake.spark3.testmodels.raw

import bio.ferlab.datalake.spark3.testmodels.normalized.{Exon, Intron, AminoAcids, CODONS}


case class RawClinvarVep(`contigName`: String = "1",
                         `start`: Long = 63411316,
                         `end`: Long = 63411317,
                         `names`: Seq[String] = Seq("rs200676709"),
                         `referenceAllele`: String = "G",
                         `alternateAlleles`: Seq[String] = Seq("A"),
                         `qual`: Option[Double] = None,
                         `filters`: Option[Seq[String]] = None,
                         `splitFromMultiAllelic`: Boolean = false,
                         `INFO_AF_EXAC`: Double = 3.4E-4,
                         `INFO_CLNVCSO`: String = "SO:0001483",
                         `INFO_GENEINFO`: String = "ALG6:29929",
                         `INFO_CLNSIGINCL`: Option[Seq[String]] = None,
                         `INFO_CLNVI`: Seq[String] = Seq("Illumina_Clinical_Services_Laboratory", "Illumina:905420"),
                         `INFO_CLNDISDB`: Seq[String] = Seq("MONDO:MONDO:0011291", "MedGen:C2930997", "OMIM:603147", "Orphanet:ORPHA79320"),
                         `INFO_CLNREVSTAT`: Seq[String] = Seq("criteria_provided", "_single_submitter"),
                         `INFO_CLNDN`: Seq[String] = Seq("Congenital_disorder_of_glycosylation_type_1C"),
                         `INFO_ALLELEID`: Int = 746612,
                         `INFO_ORIGIN`: Seq[String] = Seq("1"),
                         `INFO_SSR`: Option[Int] = None,
                         `INFO_CLNDNINCL`: Option[Seq[String]] = None,
                         `INFO_CLNSIG`: Seq[String] = Seq("Likely_benign"),
                         `INFO_RS`: Seq[String] = Seq("532466353"),
                         `INFO_DBVARID`: Option[Seq[String]] = None,
                         `INFO_AF_TGP`: Double = 6.0E-4,
                         `INFO_CLNVC`: String = "single_nucleotide_variant",
                         `INFO_CLNHGVS`: Seq[String] = Seq("NC_000001.11:g.63411316G>A"),
                         `INFO_MC`: Seq[String] = Seq("SO:0001583|missense_variant"),
                         `INFO_CLNSIGCONF`: Seq[String] = Seq("Likely_benign(1)", "Uncertain_significance(2)"),
                         `INFO_CSQ`: Seq[INFO_CSQ_VEP] = Seq(INFO_CSQ_VEP()),
                         `INFO_AF_ESP`: Double = 1.5E-4,
                         `INFO_CLNDISDBINCL`: Option[Seq[String]] = None,
                         `genotypes`: Seq[GENOTYPES] = Seq(GENOTYPES()))

case class INFO_CSQ_VEP(`Allele`: String = "C",
                    `Consequence`: Seq[String] = Seq("missense_variant"),
                    `IMPACT`: String = "MODERATE",
                    `SYMBOL`: String = "ALG6",
                    `Gene`: String = "ENSG00000088035",
                    `Feature_type`: String = "Transcript",
                    `Feature`: String = "ENST00000263440",
                    `BIOTYPE`: String = "protein_coding",
                    `EXON`: Exon = Exon(),
                    `INTRON`: Intron = Intron(),
                    `HGVSc`: String = "ENST00000263440.6:c.665G>A",
                    `HGVSp`: String = "ENSP00000263440.5:p.Gly222Asp",
                    `cDNA_position`: Int = 843,
                    `CDS_position`: Int = 807,
                    `Protein_position`: Int = 269,
                    `Amino_acids`: AminoAcids = AminoAcids(),
                    `Codons`: CODONS = CODONS(),
                    `Existing_variation`: Seq[String] = Seq("rs200676709"),
                    `DISTANCE`: Option[Int] = None,
                    `STRAND`: Int = 1,
                    `FLAGS`: Option[Seq[String]] = None,
                    `PICK`: String = "1",
                    `VARIANT_CLASS`: String = "SNV",
                    `SYMBOL_SOURCE`: String = "HGNC",
                    `HGNC_ID`: String = "HGNC:14825",
                    `CANONICAL`: String = "YES",
                    `RefSeq`: String = "NM_001005484.1&NM_001005484.1&NM_001005484.2",
                    `HGVS_OFFSET`: Option[String] = None,
                    `HGVSg`: String = "1:g.63411316G>A",
                    `CLIN_SIG`: Option[String] = None,
                    `SOMATIC`: Option[String] = None,
                    `PHENO`: Option[String] = None,
                    `PUBMED`: String = "29135816",
                    `CADD_raw_rankscore`: Option[String] = None,
                    `DANN_rankscore`: Option[String] = None,
                    `Ensembl_geneid`: Option[String] = None,
                    `Ensembl_transcriptid`: Option[String] = None,
                    `ExAC_AC`: Option[String] = None,
                    `ExAC_AF`: Option[String] = None,
                    `FATHMM_converted_rankscore`: Option[String] = None,
                    `FATHMM_pred`: Option[String] = None,
                    `GTEx_V7_tissue`: Option[String] = None,
                    `Interpro_domain`: Option[String] = None,
                    `LRT_converted_rankscore`: Option[String] = None,
                    `LRT_pred`: Option[String] = None,
                    `Polyphen2_HVAR_pred`: Option[String] = None,
                    `Polyphen2_HVAR_rankscore`: Option[String] = None,
                    `REVEL_rankscore`: Option[String] = None,
                    `SIFT_converted_rankscore`: Option[String] = None,
                    `SIFT_pred`: Option[String] = None,
                    `UK10K_AC`: Option[String] = None,
                    `UK10K_AF`: Option[String] = None,
                    `clinvar_MedGen_id`: Option[String] = None,
                    `clinvar_OMIM_id`: Option[String] = None,
                    `clinvar_Orphanet_id`: Option[String] = None,
                    `clinvar_clnsig`: Option[String] = None,
                    `clinvar_id`: Option[String] = None,
                    `clinvar_trait`: Option[String] = None,
                    `gnomAD_exomes_AC`: Option[String] = None,
                    `gnomAD_exomes_AF`: Option[String] = None,
                    `gnomAD_genomes_AC`: Option[String] = None,
                    `gnomAD_genomes_AF`: Option[String] = None,
                    `phyloP17way_primate_rankscore`: Option[String] = None,
                    `rs_dbSNP151`: Option[String] = None)
  