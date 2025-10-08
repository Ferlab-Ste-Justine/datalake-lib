package bio.ferlab.datalake.testutils.models.normalized

case class NormalizedTopmed(`chromosome`: String = "1",
                            `start`: Long = 69897,
                            `end`: Long = 69898,
                            `reference`: String = "T",
                            `alternate`: String = "C",
                            `name`: String = "TOPMed_freeze_5?chr1:10,051",
                            `ac`: Int = 2,
                            `af`: Double = 1.59276E-5,
                            `an`: Int = 125568,
                            `homozygotes`: Int = 0,
                            `heterozygotes`: Int = 2,
                            `qual`: Double = 255.0,
                            `qual_filter`: String = "FAIL")
