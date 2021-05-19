package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationWriter, DatasetConf, StorageConf, TableConf}
import bio.ferlab.datalake.spark3.loader.Format.{PARQUET, VCF}
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

object ReferenceConfiguration extends App {

  val kf_alias = "kf-strides-variant"

  val prod_storage = List(StorageConf(kf_alias, "s3a://kf-strides-variant-parquet-prd"))
  val kf_conf =
    Configuration(
      storages = prod_storage,
      sources = List(
        //raw
        DatasetConf("clinvar_vcf", kf_alias, "/raw/clinvar/clinvar.vcf.gz", VCF    , OverWrite),

        //public
        DatasetConf("1000_genomes"        , kf_alias, "/public/1000_genomes"                        , PARQUET, OverWrite, TableConf("variant", "1000_genomes")        , TableConf("variant_live", "1000_genomes")),
        DatasetConf("cancer_hotspots"     , kf_alias, "/public/cancer_hotspots"                     , PARQUET, OverWrite, TableConf("variant", "cancer_hotspots")     , TableConf("variant_live", "cancer_hotspots")),
        DatasetConf("clinvar"             , kf_alias, "/public/clinvar"                             , PARQUET, OverWrite, TableConf("variant", "clinvar")             , TableConf("variant_live", "clinvar")),
        DatasetConf("cosmic_gene_set"     , kf_alias, "/public/cosmic_gene_set"                     , PARQUET, OverWrite, TableConf("variant", "cosmic_gene_set")     , TableConf("variant_live", "cosmic_gene_set")),
        DatasetConf("dbnsfp"              , kf_alias, "/public/dbnsfp/variant"                      , PARQUET, OverWrite, TableConf("variant", "dbnsfp")              , TableConf("variant_live", "dbnsfp")),
        DatasetConf("dbnsfp_annovar"      , kf_alias, "/public/annovar/dbnsfp"                      , PARQUET, OverWrite, TableConf("variant", "dbnsfp_annovar")      , TableConf("variant_live", "dbnsfp_annovar")),
        DatasetConf("dbnsfp_original"     , kf_alias, "/public/dbnsfp/scores"                       , PARQUET, OverWrite, TableConf("variant", "dbnsfp_original")     , TableConf("variant_live", "dbnsfp_original")),
        DatasetConf("dbsnp"               , kf_alias, "/public/dbsnp"                               , PARQUET, OverWrite, TableConf("variant", "dbsnp")               , TableConf("variant_live", "dbsnp")),
        DatasetConf("ddd_gene_set"        , kf_alias, "/public/ddd_gene_set"                        , PARQUET, OverWrite, TableConf("variant", "ddd_gene_set")        , TableConf("variant_live", "ddd_gene_set")),
        DatasetConf("ensembl_mapping"     , kf_alias, "/public/ensembl_mapping"                     , PARQUET, OverWrite, TableConf("variant", "ensembl_mapping")     , TableConf("variant_live", "ensembl_mapping")),
        DatasetConf("genes"               , kf_alias, "/public/genes"                               , PARQUET, OverWrite, TableConf("variant", "genes")               , TableConf("variant_live", "genes")),
        DatasetConf("gnomad_genomes_2_1_1", kf_alias, "/public/gnomad_genomes_2_1_1_liftover_grch38", PARQUET, OverWrite, TableConf("variant", "gnomad_genomes_2_1_1"), TableConf("variant_live", "gnomad_genomes_2_1_1")),
        DatasetConf("gnomad_exomes_2_1_1" , kf_alias, "/public/gnomad_exomes_2_1_1_liftover_grch38" , PARQUET, OverWrite, TableConf("variant", "gnomad_exomes_2_1_1") , TableConf("variant_live", "gnomad_exomes_2_1_1")),
        DatasetConf("gnomad_genomes_3_0"  , kf_alias, "/public/gnomad_genomes_3_0"                  , PARQUET, OverWrite, TableConf("variant", "gnomad_genomes_3_0")  , TableConf("variant_live", "gnomad_genomes_3_0")),
        DatasetConf("human_genes"         , kf_alias, "/public/human_genes"                         , PARQUET, OverWrite, TableConf("variant", "human_genes")         , TableConf("variant_live", "human_genes")),
        DatasetConf("hpo_gene_set"        , kf_alias, "/public/hpo_gene_set"                        , PARQUET, OverWrite, TableConf("variant", "hpo_gene_set")        , TableConf("variant_live", "hpo_gene_set")),
        DatasetConf("omim_gene_set"       , kf_alias, "/public/omim_gene_set"                       , PARQUET, OverWrite, TableConf("variant", "omim_gene_set")       , TableConf("variant_live", "omim_gene_set")),
        DatasetConf("orphanet_gene_set"   , kf_alias, "/public/orphanet_gene_set"                   , PARQUET, OverWrite, TableConf("variant", "orphanet_gene_set")   , TableConf("variant_live", "orphanet_gene_set")),
        DatasetConf("topmed_bravo"        , kf_alias, "/public/topmed_bravo"                        , PARQUET, OverWrite, TableConf("variant", "topmed_bravo")        , TableConf("variant_live", "topmed_bravo"))
      ),
      sparkconf = Map("hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    )

  ConfigurationWriter.writeTo("datalake-spark3/src/main/resources/reference_kf.conf", kf_conf)
  ConfigurationWriter.writeTo("datalake-spark3/src/test/resources/config/reference_kf.conf", kf_conf)

}
