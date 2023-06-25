/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2023-06-25T12:02:16.376748
 */
package bio.ferlab.datalake.spark3.testmodels.prepared


case class PreparedVariantSugggestions(`type`: String = "variant",
                                       `locus`: String = "1-69897-T-C",
                                       `suggestion_id`: String = "314c8a3ce0334eab1a9358bcaf8c6f4206971d92",
                                       `hgvsg`: String = "chr1:g.69897T>C",
                                       `suggest`: Seq[SUGGEST] = Seq(SUGGEST(), SUGGEST(Seq("p.Ser269=", "gene2 p.Ser269=", "OR4F5 p.Ser269=", "ENSG00000186092", "transcript3", "ENST00000335137", "transcript4", "transcript2", "NM_001005484.1", "NM_001005484.2", "refseq_mrna_id1", "refseq_mrna_id2", "NP_001005277"), 2)),
                                       `chromosome`: String = "1",
                                       `rsnumber`: String = "rs200676709",
                                       `symbol_aa_change`: Seq[String] = Seq("gene2 p.Ser269=", "OR4F5 p.Ser269=", "p.Ser269="))

case class SUGGEST(`input`: Seq[String] = Seq("chr1:g.69897T>C", "1-69897-T-C", "rs200676709", "257668"),
                   `weight`: Int = 4)
