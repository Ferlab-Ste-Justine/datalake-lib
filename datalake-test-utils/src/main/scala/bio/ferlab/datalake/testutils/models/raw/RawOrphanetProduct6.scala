package bio.ferlab.datalake.testutils.models.raw

case class RawOrphanetProduct6(disorder_id: Long = 21,
                               orpha_code: Long = 447,
                               expert_link: String = "http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&Expert=447",
                               name: String = "Paroxysmal nocturnal hemoglobinuria",
                               disorder_type_id: Long = 21394,
                               disorder_type_name: String = "Disease",
                               disorder_group_id: Long = 36547,
                               disorder_group_name: String = "Disorder",
                               gene_source_of_validation: String = "22305531[PMID]",
                               gene_id: Long = 19197,
                               gene_symbol: String = "PIGA",
                               gene_name: String = "phosphatidylinositol glycan anchor biosynthesis class A",
                               gene_synonym_list: List[String] = List("GPI3", "paroxysmal nocturnal hemoglobinuria", "phosphatidylinositol N-acetylglucosaminyltransferase"),
                               ensembl_gene_id: String = "ENSG00000165195",
                               genatlas_gene_id: String = "PIGA",
                               HGNC_gene_id: String = "8957",
                               omim_gene_id: String = "311770",
                               reactome_gene_id: String = "P37287",
                               swiss_prot_gene_id: String = "P37287",
                               association_type: String = "Disease-causing somatic mutation(s) in",
                               association_type_id: Long = 17955,
                               association_status: String = "Assessed",
                               gene_locus_id: Long = 22873,
                               gene_locus: String = "Xp22.2",
                               gene_locus_key: Long = 1)
