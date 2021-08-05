package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.config.{DatasetConf, _}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locusColumNames
import bio.ferlab.datalake.spark3.loader.Format.{CSV, DELTA, PARQUET, VCF}
import bio.ferlab.datalake.spark3.loader.LoadType.{OverWrite, Upsert}

object ReferenceConfiguration extends App {

  val alias = "public_database"

  val prod_storage = List(
    StorageConf(alias, "s3a://kf-strides-variant-parquet-prd")
  )

  val kf_conf =
    Configuration(
      storages = prod_storage,
      sources = List(
        //raw
        DatasetConf("raw_clinvar", alias, "/raw/clinvar/clinvar.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
        DatasetConf("raw_dbnsfp_annovar", alias, "/raw/annovar/dbNSFP/hg38_dbnsfp41a.txt", CSV, OverWrite, readoptions = Map("sep"-> "\t", "header" -> "true", "nullValue" -> ".")),
        DatasetConf("raw_omim_genemap", alias, "/raw/omim/genemap2.txt", CSV, OverWrite, readoptions = Map("inferSchema" -> "true", "comment" -> "#", "header" -> "false", "sep" -> "\t")),

        //public
        DatasetConf("normalized_1000_genomes"        , alias, "/public/1000_genomes"                        , DELTA, OverWrite, List()         , TableConf("variant", "1000_genomes")        , TableConf("variant_live", "1000_genomes")),
        DatasetConf("normalized_cancer_hotspots"     , alias, "/public/cancer_hotspots"                     , DELTA, OverWrite, List()         , TableConf("variant", "cancer_hotspots")     , TableConf("variant_live", "cancer_hotspots")),
        DatasetConf("normalized_clinvar"             , alias, "/public/clinvar"                             , DELTA, Upsert   , locusColumNames, TableConf("variant", "clinvar")             , TableConf("variant_live", "clinvar")),
        DatasetConf("normalized_cosmic_gene_set"     , alias, "/public/cosmic_gene_set"                     , DELTA, OverWrite, List()         , TableConf("variant", "cosmic_gene_set")     , TableConf("variant_live", "cosmic_gene_set")),
        DatasetConf("normalized_dbnsfp"              , alias, "/public/dbnsfp/variant"                      , DELTA, OverWrite, List()         , TableConf("variant", "dbnsfp")              , TableConf("variant_live", "dbnsfp")),
        DatasetConf("normalized_dbnsfp_annovar"      , alias, "/public/annovar/dbnsfp"                      , DELTA, Upsert   , locusColumNames, TableConf("variant", "dbnsfp_annovar")      , TableConf("variant_live", "dbnsfp_annovar")),
        DatasetConf("normalized_dbnsfp_original"     , alias, "/public/dbnsfp/scores"                       , DELTA, OverWrite, List()         , TableConf("variant", "dbnsfp_original")     , TableConf("variant_live", "dbnsfp_original")),
        DatasetConf("normalized_dbsnp"               , alias, "/public/dbsnp"                               , DELTA, OverWrite, List()         , TableConf("variant", "dbsnp")               , TableConf("variant_live", "dbsnp")),
        DatasetConf("normalized_ddd_gene_set"        , alias, "/public/ddd_gene_set"                        , DELTA, OverWrite, List()         , TableConf("variant", "ddd_gene_set")        , TableConf("variant_live", "ddd_gene_set")),
        DatasetConf("normalized_ensembl_mapping"     , alias, "/public/ensembl_mapping"                     , DELTA, OverWrite, List()         , TableConf("variant", "ensembl_mapping")     , TableConf("variant_live", "ensembl_mapping")),
        DatasetConf("normalized_gnomad_genomes_2_1_1", alias, "/public/gnomad_genomes_2_1_1_liftover_grch38", DELTA, OverWrite, List()         , TableConf("variant", "gnomad_genomes_2_1_1"), TableConf("variant_live", "gnomad_genomes_2_1_1")),
        DatasetConf("normalized_gnomad_exomes_2_1_1" , alias, "/public/gnomad_exomes_2_1_1_liftover_grch38" , DELTA, OverWrite, List()         , TableConf("variant", "gnomad_exomes_2_1_1") , TableConf("variant_live", "gnomad_exomes_2_1_1")),
        DatasetConf("normalized_gnomad_genomes_3_0"  , alias, "/public/gnomad_genomes_3_0"                  , DELTA, OverWrite, List()         , TableConf("variant", "gnomad_genomes_3_0")  , TableConf("variant_live", "gnomad_genomes_3_0")),
        DatasetConf("normalized_human_genes"         , alias, "/public/human_genes"                         , DELTA, OverWrite, List()         , TableConf("variant", "human_genes")         , TableConf("variant_live", "human_genes")),
        DatasetConf("normalized_hpo_gene_set"        , alias, "/public/hpo_gene_set"                        , DELTA, OverWrite, List()         , TableConf("variant", "hpo_gene_set")        , TableConf("variant_live", "hpo_gene_set")),
        DatasetConf("normalized_omim_gene_set"       , alias, "/public/omim_gene_set"                       , DELTA, OverWrite, List()         , TableConf("variant", "omim_gene_set")       , TableConf("variant_live", "omim_gene_set")),
        DatasetConf("normalized_orphanet_gene_set"   , alias, "/public/orphanet_gene_set"                   , DELTA, OverWrite, List()         , TableConf("variant", "orphanet_gene_set")   , TableConf("variant_live", "orphanet_gene_set")),
        DatasetConf("normalized_topmed_bravo"        , alias, "/public/topmed_bravo"                        , DELTA, OverWrite, List()         , TableConf("variant", "topmed_bravo")        , TableConf("variant_live", "topmed_bravo")),

        DatasetConf("enriched_genes"                 , alias, "/public/genes"                               , DELTA, OverWrite, List()         , TableConf("variant", "genes")               , TableConf("variant_live", "genes")),

      ),
      sparkconf = Map("hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    )

  ConfigurationWriter.writeTo("datalake-spark3/src/main/resources/reference_kf.conf", kf_conf)
  ConfigurationWriter.writeTo("datalake-spark3/src/test/resources/config/reference_kf.conf", kf_conf)

}
