package bio.ferlab.datalake.spark3.publictables

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA, GFF, VCF, XML}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import bio.ferlab.datalake.spark3.publictables.PublicDatasets.gnomadStorageId


case class PublicDatasets(alias: String, tableDatabase: Option[String], viewDatabase: Option[String]) extends BaseDatasets {
  val sources: List[DatasetConf] = List(
          //raw
          DatasetConf("raw_clinvar"                  , alias, "/raw/landing/clinvar/clinvar.vcf.gz"                                , VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
          DatasetConf("raw_dbsnp"                    , alias, "/raw/landing/dbsnp/GCF_000001405.40.gz"                             , VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
          DatasetConf("raw_gnomad_genomes_v3"        , alias, "/release/3.1/vcf/genomes/gnomad.genomes.v3.1.sites.chr[^M]*.vcf.bgz", VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")).copy(storageid = gnomadStorageId),
          DatasetConf("raw_gnomad_joint_v4"          , alias, "/raw/landing/gnomad_v4/release/4.1/vcf/joint/gnomad.joint.v4.1.sites.chr[^M]*.vcf.bgz",  VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
          DatasetConf("raw_gnomad_cnv_v4"            , alias, "/raw/landing/gnomad_v4/release/4.1/exome_cnv/gnomad.v4.1.cnv.all.vcf.gz",                VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
          DatasetConf("raw_gnomad_constraint_v2_1_1" , alias, "/raw/landing/gnomad_v2_1_1/gnomad.v2.1.1.lof_metrics.by_gene.txt.gz", CSV  , OverWrite , readoptions = Map("header" -> "true", "sep" -> "\t")),
          DatasetConf("raw_topmed_bravo"             , alias, "/raw/landing/topmed/bravo-dbsnp-*.vcf.gz"                           , VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
          DatasetConf("raw_1000_genomes"             , alias, "/raw/landing/1000Genomes/ALL.*.sites.vcf.gz"                        , VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
          DatasetConf("raw_dbnsfp"                   , alias, "/raw/landing/dbNSFP/dbNSFP4.3a_variant.chr*.gz"                     , CSV  , OverWrite , readoptions = Map("sep" -> "\t", "header" -> "true", "nullValue" -> ".")),
          DatasetConf("raw_dbnsfp_annovar"           , alias, "/raw/landing/annovar/dbNSFP/hg38_dbnsfp41a.txt"                     , CSV  , OverWrite , readoptions = Map("sep" -> "\t", "header" -> "true", "nullValue" -> ".")),
          DatasetConf("raw_omim_gene_set"            , alias, "/raw/landing/omim/genemap2.txt"                                     , CSV  , OverWrite , readoptions = Map("inferSchema" -> "true", "comment" -> "#", "header" -> "false", "sep" -> "\t")),
          DatasetConf("raw_orphanet_gene_association", alias, "/raw/landing/orphanet/en_product6.xml"                              , XML  , OverWrite),
          DatasetConf("raw_orphanet_disease_history" , alias, "/raw/landing/orphanet/en_product9_ages.xml"                         , XML  , OverWrite),
          DatasetConf("raw_cosmic_gene_set"          , alias, "/raw/landing/cosmic/Cosmic_CancerGeneCensus_GRCh38.tsv.gz"          , CSV  , OverWrite , readoptions = Map("header" -> "true", "sep" -> "\t")),
          DatasetConf("raw_cosmic_mutation_set"      , alias, "/raw/landing/cosmic/cmc_export.tsv.gz"                              , CSV  , OverWrite , readoptions = Map("header" -> "true", "sep" -> "\t")),
          DatasetConf("raw_ddd_gene_set"             , alias, "/raw/landing/ddd/DDG2P.csv.gz"                                      , CSV  , OverWrite , readoptions = Map("header" -> "true")),
          DatasetConf("raw_hpo_gene_set"             , alias, "/raw/landing/hpo/genes_to_phenotype.txt"                            , CSV  , OverWrite , readoptions = Map("inferSchema" -> "true", "comment" -> "#", "header" -> "false", "sep" -> "\t", "nullValue" -> "-")),
          DatasetConf("raw_refseq_human_genes"       , alias, "/raw/landing/refseq/Homo_sapiens.gene_info.gz"                      , CSV  , OverWrite , readoptions = Map("inferSchema" -> "true", "header" -> "true", "sep" -> "\t", "nullValue" -> "-")),
          DatasetConf("raw_refseq_annotation"        , alias, "/raw/landing/refseq/GCF_GRCh38_genomic.gff.gz"                      , GFF  , OverWrite),
          DatasetConf("raw_ensembl_entrez"           , alias, "/raw/landing/ensembl/Homo_sapiens.GRCh38.entrez.tsv.gz"             , CSV  , OverWrite , readoptions = Map("header" -> "true", "sep" -> "\t")),
          DatasetConf("raw_ensembl_refseq"           , alias, "/raw/landing/ensembl/Homo_sapiens.GRCh38.refseq.tsv.gz"             , CSV  , OverWrite , readoptions = Map("header" -> "true", "sep" -> "\t")),
          DatasetConf("raw_ensembl_uniprot"          , alias, "/raw/landing/ensembl/Homo_sapiens.GRCh38.uniprot.tsv.gz"            , CSV  , OverWrite , readoptions = Map("header" -> "true", "sep" -> "\t")),
          DatasetConf("raw_ensembl_ena"              , alias, "/raw/landing/ensembl/Homo_sapiens.GRCh38.ena.tsv.gz"                , CSV  , OverWrite , readoptions = Map("header" -> "true", "sep" -> "\t")),
          DatasetConf("raw_ensembl_gff"              , alias, "/raw/landing/ensembl/Homo_sapiens.GRCh38.gff.gz"                    , GFF  , OverWrite),
          DatasetConf("raw_spliceai_indel"           , alias, "/raw/landing/spliceai/spliceai_scores.raw.indel.hg38.vcf.gz"        , VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
          DatasetConf("raw_spliceai_snv"             , alias, "/raw/landing/spliceai/spliceai_scores.raw.snv.hg38.vcf.gz"          , VCF  , OverWrite , readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),

          //public
          DatasetConf("normalized_1000_genomes"            , alias, "/public/1000_genomes"                         , DELTA, OverWrite , partitionby = List()            , table = table("1000_genomes")             , view = view("variant_live")),
          DatasetConf("normalized_cancer_hotspots"         , alias, "/public/cancer_hotspots"                      , DELTA, OverWrite , partitionby = List()            , table = table("cancer_hotspots")          , view = view("cancer_hotspots")),
          DatasetConf("normalized_clinvar"                 , alias, "/public/clinvar"                              , DELTA, OverWrite , partitionby = List()            , repartition=Some(Coalesce()), table = table("clinvar")                  , view = view("clinvar")),
          DatasetConf("normalized_cosmic_gene_set"         , alias, "/public/cosmic_gene_set"                      , DELTA, OverWrite , partitionby = List()            , table = table("cosmic_gene_set")          , view = view("cosmic_gene_set")),
          DatasetConf("normalized_cosmic_mutation_set"     , alias, "/public/cosmic_mutation_set"                  , DELTA, OverWrite , partitionby = List()            , table = table("cosmic_mutation_set")      , view = view("cosmic_mutation_set")),
          DatasetConf("normalized_dbnsfp"                  , alias, "/public/dbnsfp/variant"                       , DELTA, OverWrite , partitionby = List("chromosome"), table = table("dbnsfp")                   , view = view("dbnsfp")),
          DatasetConf("normalized_dbnsfp_annovar"          , alias, "/public/annovar/dbnsfp"                       , DELTA, OverWrite , partitionby = List("chromosome"), table = table("dbnsfp_annovar")           , view = view("dbnsfp_annovar")),
          DatasetConf("normalized_dbsnp"                   , alias, "/public/dbsnp"                                , DELTA, OverWrite , partitionby = List("chromosome"), table = table("dbsnp")                    , view = view("dbsnp")),
          DatasetConf("normalized_ddd_gene_set"            , alias, "/public/ddd_gene_set"                         , DELTA, OverWrite , partitionby = List()            , table = table("ddd_gene_set")             , view = view("ddd_gene_set")),
          DatasetConf("normalized_ensembl_mapping"         , alias, "/public/ensembl_mapping"                      , DELTA, OverWrite , partitionby = List()            , table = table("ensembl_mapping")          , view = view("ensembl_mapping"), repartition = Some(Coalesce())),
          DatasetConf("normalized_gnomad_genomes_v2_1_1"   , alias, "/public/gnomad_genomes_v2_1_1_liftover_grch38", DELTA, OverWrite , partitionby = List("chromosome"), table = table("gnomad_genomes_v2_1_1")    , view = view("gnomad_genomes_v2_1_1")),
          DatasetConf("normalized_gnomad_exomes_v2_1_1"    , alias, "/public/gnomad_exomes_v2_1_1_liftover_grch38" , DELTA, OverWrite , partitionby = List("chromosome"), table = table("gnomad_exomes_v2_1_1")     , view = view("gnomad_exomes_v2_1_1")),
          DatasetConf("normalized_gnomad_constraint_v2_1_1", alias, "/public/gnomad_constraint_v2_1_1"             , DELTA, OverWrite , partitionby = List("chromosome"), table = table("gnomad_constraint_v_2_1_1"), view = view("gnomad_constraint_v_2_1_1")),
          DatasetConf("normalized_gnomad_genomes_v3"       , alias, "/public/gnomad_genomes_v3"                    , DELTA, OverWrite , partitionby = List("chromosome"), table = table("gnomad_genomes_v3")        , view = view("gnomad_genomes_v3")),
          DatasetConf("normalized_gnomad_joint_v4"         , alias, "/public/gnomad_joint_v4"                      , DELTA, OverWrite , partitionby = List("chromosome"), table = table("gnomad_joint_v4")          , view = view("gnomad_joint_v4")),
          DatasetConf("normalized_gnomad_cnv_v4"           , alias, "/public/gnomad_cnv_v4"                        , DELTA, OverWrite , partitionby = List("chromosome"), table = table("gnomad_cnv_v4")            , view = view("gnomad_cnv_v4")),
          DatasetConf("normalized_human_genes"             , alias, "/public/human_genes"                          , DELTA, OverWrite , partitionby = List()            , table = table("human_genes")              , view = view("human_genes")),
          DatasetConf("normalized_hpo_gene_set"            , alias, "/public/hpo_gene_set"                         , DELTA, OverWrite , partitionby = List()            , table = table("hpo_gene_set")             , view = view("hpo_gene_set")),
          DatasetConf("normalized_omim_gene_set"           , alias, "/public/omim_gene_set"                        , DELTA, OverWrite , partitionby = List()            , table = table("omim_gene_set")            , view = view("omim_gene_set")),
          DatasetConf("normalized_orphanet_gene_set"       , alias, "/public/orphanet_gene_set"                    , DELTA, OverWrite , partitionby = List()            , table = table("orphanet_gene_set")        , view = view("orphanet_gene_set")),
          DatasetConf("normalized_topmed_bravo"            , alias, "/public/topmed_bravo"                         , DELTA, OverWrite , partitionby = List()            , table = table("topmed_bravo")             , view = view("topmed_bravo")),
          DatasetConf("normalized_refseq_annotation"       , alias, "/public/refseq_annotation"                    , DELTA, OverWrite , partitionby = List("chromosome"), table = table("refseq_annotation")        , view = view("refseq_annotation")),
          DatasetConf("normalized_spliceai_indel"          , alias, "/public/spliceai/indel"                       , DELTA, OverWrite , partitionby = List("chromosome"), table = table("spliceai_indel")           , view = view("spliceai_indel")),
          DatasetConf("normalized_spliceai_snv"            , alias, "/public/spliceai/snv"                         , DELTA, OverWrite , partitionby = List("chromosome"), table = table("spliceai_snv")             , view = view("spliceai_snv")),

          // enriched
          DatasetConf("enriched_genes"                     , alias, "/public/genes"                  , DELTA, OverWrite , partitionby = List()            , table = table("genes")                , view = view("genes")),
          DatasetConf("enriched_dbnsfp"                    , alias, "/public/dbnsfp/scores"          , DELTA, OverWrite , partitionby = List("chromosome"), table = table("dbnsfp_original")      , view = view("dbnsfp_original")),
          DatasetConf("enriched_spliceai_indel"            , alias, "/public/spliceai/enriched/indel", DELTA, OverWrite , partitionby = List("chromosome"), repartition= Some(RepartitionByRange(columnNames = Seq("chromosome", "start"))), table = table("spliceai_enriched_indel"), view = view("spliceai_enriched_indel")),
          DatasetConf("enriched_spliceai_snv"              , alias, "/public/spliceai/enriched/snv"  , DELTA, OverWrite , partitionby = List("chromosome"), repartition= Some(RepartitionByRange(columnNames = Seq("chromosome", "start"))), table = table("spliceai_enriched_snv")  , view = view("spliceai_enriched_snv")),
          DatasetConf("enriched_rare_variant"              , alias, "/public/rare_variant/enriched"  , DELTA, OverWrite , partitionby = List("chromosome", "is_rare"), table = table("rare_variant_enriched"), view = view("rare_variant_enriched"))

  )


}

object PublicDatasets{
  val gnomadStorageId: String = "gnomad"
  val gnomadStorage: StorageConf = StorageConf(gnomadStorageId, "s3a://gnomad-public-us-east-1", S3)
}
