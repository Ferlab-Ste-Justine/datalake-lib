package bio.ferlab.datalake.testutils.models.normalized

case class NormalizedHumanGenes(`tax_id`: Int = 9606,
                                `entrez_gene_id`: Int = 777,
                                `symbol`: String = "OR4F5",
                                `locus_tag`: Option[String] = None,
                                `synonyms`: List[String] = List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139"),
                                `external_references`: Map[String, String] = Map("mim" -> "601013", "hgnc" -> "HGNC:1392", "ensembl" -> "ENSG00000198216"),
                                `chromosome`: String = "1",
                                `map_location`: String = "1q25.3",
                                `description`: String = "calcium voltage-gated channel subunit alpha1 E",
                                `type_of_gene`: String = "protein-coding",
                                `symbol_from_nomenclature_authority`: String = "CACNA1E",
                                `full_name_from_nomenclature_authority`: String = "calcium voltage-gated channel subunit alpha1 E",
                                `nomenclature_status`: String = "O",
                                `other_designations`: List[String] = List("voltage-dependent R-type calcium channel subunit alpha-1E", "brain calcium channel II", "calcium channel", "L type, alpha-1 polypeptide", "calcium channel, R type, alpha-1 polypeptide", "voltage-dependent calcium channel alpha 1E subunit", "voltage-gated calcium channel alpha subunit Cav2.3"),
                                `feature_types`: Option[Map[String, String]] = None,
                                `ensembl_gene_id`: String = "ENSG00000198216",
                                `omim_gene_id`: String = "601013")
