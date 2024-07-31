package bio.ferlab.datalake.testutils.models.normalized

case class NormalizedSpliceAi(`chromosome`: String = "2",
                              `start`: Long = 210862942,
                              `end`: Long = 210862946,
                              `reference`: String = "GGCA",
                              `alternate`: String = "G",
                              `allele`: String = "G",
                              `symbol`: String = "KCNH1",
                              `ds_ag`: Double = 0.1,
                              `ds_al`: Double = 0.1,
                              `ds_dg`: Double = 0.1,
                              `ds_dl`: Double = 0.1,
                              `dp_ag`: Int = -3,
                              `dp_al`: Int = 36,
                              `dp_dg`: Int = 32,
                              `dp_dl`: Int = 22)
