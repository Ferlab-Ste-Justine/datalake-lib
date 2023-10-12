package bio.ferlab.datalake.testutils.models.normalized

case class NormalizedHpoGeneSet(`entrez_gene_id`: Int = 777,
                                `symbol`: String = "CACNA1E",
                                `hpo_term_id`: String = "HP:0001347",
                                `hpo_term_name`: String = "Hyperreflexia",
                                `frequency_raw`: Option[String] = None,
                                `frequency_hpo`: Option[String] = None,
                                `source_info`: Option[String] = None,
                                `source`: String = "mim2gene",
                                `source_id`: String = "OMIM:618285",
                                `ensembl_gene_id`: String = "ENSG00000198216")
