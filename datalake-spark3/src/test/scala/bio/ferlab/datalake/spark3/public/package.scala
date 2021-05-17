package bio.ferlab.datalake.spark3

import bio.ferlab.datalake.spark3.config.{Configuration, SourceConf, StorageConf}
import bio.ferlab.datalake.spark3.loader.Format.PARQUET
import bio.ferlab.datalake.spark3.loader.LoadType.OverWrite

package object public {

  val alias = "test-alias"

  implicit val conf: Configuration =
    Configuration(
      storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile)),
      sources = List(
        SourceConf(alias, "/public/1000_genomes"                        , "variant", "1000_genomes"        , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/cancer_hotspots"                     , "variant", "cancer_hotspots"     , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/clinvar"                             , "variant", "clinvar"             , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/cosmic_gene_set"                     , "variant", "cosmic_gene_set"     , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/dbnsfp/variant"                      , "variant", "dbnsfp"              , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/annovar/dbnsfp"                      , "variant", "dbnsfp_annovar"      , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/dbnsfp/scores"                       , "variant", "dbnsfp_original"     , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/dbsnp"                               , "variant", "dbsnp"               , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/ddd_gene_set"                        , "variant", "ddd_gene_set"        , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/ensembl_mapping"                     , "variant", "ensembl_mapping"     , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/genes"                               , "variant", "genes"               , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/gnomad_genomes_2_1_1_liftover_grch38", "variant", "gnomad_genomes_2_1_1", PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/gnomad_exomes_2_1_1_liftover_grch38" , "variant", "gnomad_exomes_2_1_1" , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/gnomad_genomes_3_0"                  , "variant", "gnomad_genomes_3_0"  , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/human_genes"                         , "variant", "human_genes"         , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/hpo_gene_set"                        , "variant", "hpo_gene_set"        , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/omim_gene_set"                       , "variant", "omim_gene_set"       , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/orphanet_gene_set"                   , "variant", "orphanet_gene_set"   , PARQUET, OverWrite, List()),
        SourceConf(alias, "/public/topmed_bravo"                        , "variant", "topmed_bravo"        , PARQUET, OverWrite, List())
      )
    )

}
