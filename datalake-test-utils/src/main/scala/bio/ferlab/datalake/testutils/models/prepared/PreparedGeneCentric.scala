/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2023-06-25T14:15:46.921127
 */
package bio.ferlab.datalake.testutils.models.prepared

case class PreparedGeneCentric(`symbol`: String = "OR4F5",
                               `entrez_gene_id`: Int = 777,
                               `omim_gene_id`: String = "601013",
                               `hgnc`: String = "HGNC:1392",
                               `ensembl_gene_id`: String = "ENSG00000198216",
                               `location`: String = "1q25.3",
                               `name`: String = "calcium voltage-gated channel subunit alpha1 E",
                               `alias`: Seq[String] = Seq("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139"),
                               `biotype`: String = "protein_coding",
                               `orphanet`: Seq[ORPHANET] = Seq(ORPHANET()),
                               `hpo`: Seq[HPO] = Seq(HPO()),
                               `omim`: Seq[OMIM] = Seq(OMIM()),
                               `chromosome`: String = "1",
                               `ddd`: Seq[DDD] = Seq(DDD()),
                               `cosmic`: Seq[COSMIC] = Seq(COSMIC()),
                               `gnomad`: GNOMAD = GNOMAD(),
                               `hash`: String = "9b8016c31b93a7504a8314ce3d060792f67ca2ad",
                               `search_text`: Seq[String] = Seq("OR4F5", "ENSG00000198216", "BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139")
                              )

case class ORPHANET(`disorder_id`: Long = 17827,
                    `panel`: String = "Immunodeficiency due to a classical component pathway complement deficiency",
                    `inheritance`: Seq[String] = Seq("Autosomal recessive"))

case class DDD(`disease_name`: String = "OCULOAURICULAR SYNDROME")

case class GNOMAD(`pli`: Float = 1.0f,
                  `loeuf`: Float = 0.054f)

case class COSMIC(`tumour_types_germline`: Seq[String] = Seq("breast", "colon", "endometrial cancer under age 50"))

case class OMIM(`name`: String = "Epileptic encephalopathy, early infantile, 69",
                `omim_id`: String = "618285",
                `inheritance`: Seq[String] = Seq("Autosomal dominant"),
                `inheritance_code`: Seq[String] = Seq("AD"))

case class HPO(`hpo_term_id`: String = "HP:0001347",
               `hpo_term_name`: String = "Hyperreflexia",
               `hpo_term_label`: String = "Hyperreflexia (HP:0001347)")
