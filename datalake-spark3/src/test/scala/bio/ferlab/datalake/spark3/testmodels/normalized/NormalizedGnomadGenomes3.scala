package bio.ferlab.datalake.spark3.testmodels.normalized

case class NormalizedGnomadGenomes3(chromosome: String = "1",
                                    start: Long = 69897,
                                    end: Long = 69899,
                                    reference: String = "T",
                                    alternate: String = "C",
                                    qual: Double = 0.5,
                                    name: String = "BRAF",
                                    ac: Long = 10,
                                    ac_raw: Long = 11,
                                    af: Double = 0.5,
                                    af_raw: Double = 0.6,
                                    an: Long = 20,
                                    an_raw: Long = 21,
                                    nhomalt: Long = 10,
                                    nhomalt_raw: Long = 11)
