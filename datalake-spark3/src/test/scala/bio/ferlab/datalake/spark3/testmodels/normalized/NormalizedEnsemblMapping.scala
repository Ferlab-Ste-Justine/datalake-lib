package bio.ferlab.datalake.spark3.testmodels.normalized

case class NormalizedEnsemblMapping(
                                 `ensembl_gene_id`: String = "ENSG00000284662",
                                 `ensembl_transcript_id`: String = "ENST00000332831",
                                 `tags`: List[String] =
                                 List("MANE Select v0.93", "MANE Plus Clinical v0.93", "Ensembl Canonical"),
                                 `refseq`: List[REFSEQ] = List(REFSEQ(), REFSEQ("NM_001005277", "RefSeq_mRNA")),
                                 `entrez`: List[ENTREZ] = List(ENTREZ()),
                                 `uniprot`: List[UNIPROT] = List(UNIPROT()),
                                 `species`: String = "Homo_sapiens",
                                 `tax_id`: String = "9606",
                                 `primary_accessions`: List[String] = List("chr1", "CM000663"),
                                 `secondary_accessions`: List[String] = List("ALI87340"),
                                 `is_canonical`: Boolean = true,
                                 `is_mane_select`: Boolean = true,
                                 `is_mane_plus`: Boolean = true,
                                 `refseq_mrna_id`: String = "NM_001005277",
                                 `refseq_protein_id`: String = "NP_001005277",
                                 `genome_build`: String = "GRCh38",
                                 `ensembl_release_id`: Int = 104
                               )

case class REFSEQ(`id`: String = "NP_001005277", `database`: String = "RefSeq_peptide")

case class ENTREZ(`id`: String = "81399", `database`: String = "EntrezGene")

case class UNIPROT(`id`: String = "Q6IEY1", `database`: String = "Uniprot/SWISSPROT")