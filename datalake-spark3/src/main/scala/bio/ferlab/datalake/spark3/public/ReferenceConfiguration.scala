package bio.ferlab.datalake.spark3.public

import bio.ferlab.datalake.spark3.config.{Configuration, ConfigurationWriter, SourceConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.PARQUET
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

object ReferenceConfiguration extends App {

  val kf_alias = "kf-strides-variant"
  val kf_conf =
    Configuration(
      storages = List(StorageConf("kf-strides-variant", "s3a://kf-strides-variant-parquet-prd")),
      sources = List(
          SourceConf(kf_alias, "/public/1000_genomes"                        , "variant", "1000_genomes"        , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/cancer_hotspots"                     , "variant", "cancer_hotspots"     , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/clinvar"                             , "variant", "clinvar"             , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/cosmic_gene_set"                     , "variant", "cosmic_gene_set"     , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/dbnsfp/variant"                      , "variant", "dbnsfp"              , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/annovar/dbnsfp"                      , "variant", "dbnsfp_annovar"      , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/dbnsfp/scores"                       , "variant", "dbnsfp_original"     , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/dbsnp"                               , "variant", "dbsnp"               , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/ddd_gene_set"                        , "variant", "ddd_gene_set"        , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/ensembl_mapping"                     , "variant", "ensembl_mapping"     , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/genes"                               , "variant", "genes"               , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/gnomad_genomes_2_1_1_liftover_grch38", "variant", "gnomad_genomes_2_1_1", PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/gnomad_exomes_2_1_1_liftover_grch38" , "variant", "gnomad_exomes_2_1_1" , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/gnomad_genomes_3_0"                  , "variant", "gnomad_genomes_3_0"  , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/human_genes"                         , "variant", "human_genes"         , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/hpo_gene_set"                        , "variant", "hpo_gene_set"        , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/omim_gene_set"                       , "variant", "omim_gene_set"       , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/orphanet_gene_set"                   , "variant", "orphanet_gene_set"   , PARQUET, OverWrite),
          SourceConf(kf_alias, "/public/topmed_bravo"                        , "variant", "topmed_bravo"        , PARQUET, OverWrite)
      ),
      sparkconf = Map("hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    )

  ConfigurationWriter.writeTo("datalake-spark3/src/main/resources/reference_kf.conf", kf_conf)

}
