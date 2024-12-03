/**
 * Generated by [[bio.ferlab.datalake.spark3.utils.ClassGenerator]]
 * on 2021-02-26T14:31:57.067
 */
package bio.ferlab.datalake.testutils.models.enriched

import bio.ferlab.datalake.testutils.models.frequency.{Frequency, GlobalFrequency}

import EnrichedVariant._

case class EnrichedVariant(chromosome: String = "1",
                           start: Long = 69897,
                           end: Long = 69898,
                           reference: String = "T",
                           alternate: String = "C",
                           locus: String = "1-69897-T-C",
                           hash: String = "314c8a3ce0334eab1a9358bcaf8c6f4206971d92",
                           hgvsg: String = "chr1:g.69897T>C",
                           variant_class: String = "SNV",
                           assembly_version: String = "GRCh38",
                           frequency: GlobalFrequency = GlobalFrequency(
                             total = Frequency(ac = 4, pc = 2, hom = 2, an = 4, pn = 2, af = 1.0, pf = 1.0),
                             zygosities = Set("HOM")
                           ),
                           external_frequencies: FREQUENCIES = FREQUENCIES(),
                           clinvar: CLINVAR = CLINVAR(),
                           rsnumber: String = "rs200676709",
                           dna_change: String = "T>C",
                           genes: List[GENES] = List(GENES()),
                           cmc: CMC = CMC(),
                           variant_external_reference: List[String] = List("DBSNP", "Clinvar", "Cosmic", "gnomADv4"),
                           gene_external_reference: List[String] = List("HPO", "Orphanet", "OMIM", "DDD", "Cosmic", "gnomAD", "SpliceAI"),
                          )

object EnrichedVariant {

  case class FREQUENCIES(thousand_genomes: ThousandGenomesFreq = ThousandGenomesFreq(3446, 5008, 0.688099),
                         topmed_bravo: TopmedFreq = TopmedFreq(2, 125568, 0.0000159276, 0, 2),
                         gnomad_genomes_2_1_1: GnomadFreqOutput = GnomadFreqOutput(1, 26342, 0.000037962189659099535, 0),
                         gnomad_exomes_2_1_1: GnomadFreqOutput = GnomadFreqOutput(0, 2, 0.0, 0),
                         gnomad_genomes_3: GnomadFreqOutput = GnomadFreqOutput(10, 20, 0.5, 10),
                         gnomad_genomes_4: GnomadFreqOutput = GnomadFreqOutput(0, 6022, 0.0, 0))


  case class ThousandGenomesFreq(ac: Long = 10,
                                 an: Long = 20,
                                 af: Double = 0.5)

  case class GnomadFreqOutput(ac: Long = 10,
                              an: Long = 20,
                              af: Double = 0.5,
                              hom: Long = 10)

  case class TopmedFreq(ac: Long = 10,
                        an: Long = 20,
                        af: Double = 0.5,
                        hom: Long = 10,
                        het: Long = 10)

  case class CLINVAR(clinvar_id: String = "257668",
                     clin_sig: List[String] = List("Benign"),
                     conditions: List[String] = List("Congenital myasthenic syndrome 12", "not specified", "not provided"),
                     inheritance: List[String] = List("germline"),
                     interpretations: List[String] = List("Benign"))

  case class GENES(symbol: Option[String] = Some("OR4F5"),
                   entrez_gene_id: Option[Int] = Some(777),
                   omim_gene_id: Option[String] = Some("601013"),
                   hgnc: Option[String] = Some("HGNC:1392"),
                   ensembl_gene_id: Option[String] = Some("ENSG00000198216"),
                   location: Option[String] = Some("1q25.3"),
                   name: Option[String] = Some("calcium voltage-gated channel subunit alpha1 E"),
                   alias: Option[List[String]] = Some(List("BII", "CACH6", "CACNL1A6", "Cav2.3", "EIEE69", "gm139")),
                   biotype: Option[String] = Some("protein_coding"),
                   orphanet: List[ORPHANET] = List(ORPHANET()),
                   hpo: List[HPO] = List(HPO()),
                   omim: List[OMIM] = List(OMIM()),
                   ddd: List[DDD] = List(DDD()),
                   cosmic: List[COSMIC] = List(COSMIC()),
                   spliceai: Option[SPLICEAI] = Some(SPLICEAI()),
                   gnomad: Option[GNOMAD] = Some(GNOMAD())
                  )


  case class SPLICEAI(ds: Double = 0.1,
                      `type`: Option[Seq[String]] = Some(Seq("AG", "AL", "DG", "DL")))

  case class CMC(mutation_url: String = "https://cancer.sanger.ac.uk/cosmic/mutation/overview?id=29491889&genome=37",
                 shared_aa: Int = 9,
                 cosmic_id: String = "COSV59205318",
                 sample_mutated: Int = 699,
                 sample_tested: Int = 86821,
                 tier: String = "2",
                 sample_ratio: Double = 0.008051047557618549
                )

}