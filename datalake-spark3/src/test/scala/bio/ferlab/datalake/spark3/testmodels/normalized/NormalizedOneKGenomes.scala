package bio.ferlab.datalake.spark3.testmodels.normalized

case class NormalizedOneKGenomes(`chromosome`: String = "1",
                                 `start`: Long = 69897,
                                 `end`: Long = 69898,
                                 `name`: String = "rs200676709",
                                 `reference`: String = "T",
                                 `alternate`: String = "C",
                                 `ac`: Int = 3446,
                                 `af`: Double = 0.688099,
                                 `an`: Int = 5008,
                                 `afr_af`: Double = 0.407,
                                 `eur_af`: Double = 0.7942,
                                 `sas_af`: Double = 0.8098,
                                 `amr_af`: Double = 0.6254,
                                 `eas_af`: Double = 0.876,
                                 `dp`: Int = 22289)
