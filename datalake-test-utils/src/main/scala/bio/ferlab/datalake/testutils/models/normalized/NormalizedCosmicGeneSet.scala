/**
 * Generated by [[bio.ferlab.datalake.testutils.ClassGenerator]]
 * on 2023-08-29T16:34:34.753842
 */
package bio.ferlab.datalake.testutils.models.normalized




case class NormalizedCosmicGeneSet(`chromosome`: String = "10",
                                   `start`: Long = 43077027,
                                   `symbol`: String = "RET",
                                   `name`: String = "ret proto-oncogene",
                                   `cosmic_gene_id`: String = "COSG68325",
                                   `tier`: Int = 1,
                                   `chr_band`: String = "11.21",
                                   `somatic`: Boolean = true,
                                   `germline`: Boolean = true,
                                   `tumour_types_somatic`: Seq[String] = Seq("medullary thyroid", "papillary thyroid", "pheochromocytoma", "NSCLC", "Spitzoid tumour"),
                                   `tumour_types_germline`: Seq[String] = Seq("medullary thyroid", "papillary thyroid", "pheochromocytoma"),
                                   `cancer_syndrome`: String = "multiple endocrine neoplasia 2A/2B",
                                   `tissue_type`: Seq[String] = Seq("E", "O"),
                                   `molecular_genetics`: String = "Dom",
                                   `role_in_cancer`: Seq[String] = Seq("oncogene", "fusion"),
                                   `mutation_types`: Seq[String] = Seq("T", "Mis", "N", "F"),
                                   `translocation_partner`: Seq[String] = Seq("H4", "PRKAR1A", "NCOA4", "PCM1", "GOLGA5", "TRIM33", "KTN1", "TRIM27", "HOOK3", "KIF5B", "CCDC6"),
                                   `other_germline_mutation`: Boolean = true,
                                   `other_syndrome`: Seq[String] = Seq("Hirschsprung disease"),
                                   `synonyms`: Seq[String] = Seq("RET", "ENSG00000165731.18", "P07949", "5979", "CDHF12", "CDHR16", "PTC", "RET51"))

