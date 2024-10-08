/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-02-15T16:13:44.624
 */
package bio.ferlab.datalake.testutils.models.enriched

case class EnrichedGenes(`symbol`: String = "OR4F5",
                         `entrez_gene_id`: Int = 777,
                         `omim_gene_id`: String = "601013",
                         `hgnc`: String = "HGNC:1392",
                         `ensembl_gene_id`: String = "ENSG00000198216",
                         `location`: String = "1q25.3",
                         `name`: String = "calcium voltage-gated channel subunit alpha1 E",
                         `alias`: List[String] = List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139"),
                         `biotype`: String = "protein_coding",
                         `orphanet`: List[ORPHANET] = List(ORPHANET()),
                         `hpo`: List[HPO] = List(HPO()),
                         `omim`: List[OMIM] = List(OMIM()),
                         `chromosome`: String = "1",
                         `ddd`: List[DDD] = List(DDD()),
                         `cosmic`: List[COSMIC] = List(COSMIC()),
                         gnomad: Option[GNOMAD] = Some(GNOMAD()))

case class ORPHANET(`disorder_id`: Long = 17827,
                    `panel`: String = "Immunodeficiency due to a classical component pathway complement deficiency",
                    `inheritance`: List[String] = List("Autosomal recessive"))

case class HPO(`hpo_term_id`: String = "HP:0001347",
               `hpo_term_name`: String = "Hyperreflexia",
               `hpo_term_label`: String = "Hyperreflexia (HP:0001347)")

case class OMIM(`name`: String = "Epileptic encephalopathy, early infantile, 69",
                `omim_id`: String = "618285",
                `inheritance`: List[String] = List("Autosomal dominant"),
                `inheritance_code`: List[String] = List("AD"))

case class DDD(`disease_name`: String = "OCULOAURICULAR SYNDROME")

case class COSMIC(`tumour_types_germline`: List[String] = List("breast", "colon", "endometrial cancer under age 50"))

case class GNOMAD(pli: Float = 1.0f,
                  loeuf: Float = 0.054f)
