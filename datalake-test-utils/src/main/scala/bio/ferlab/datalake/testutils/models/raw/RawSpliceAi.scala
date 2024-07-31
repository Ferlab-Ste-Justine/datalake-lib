package bio.ferlab.datalake.testutils.models.raw

case class RawSpliceAi(`contigName`: String = "1",
                       `start`: Long = 210862941,
                       `end`: Long = 210862945,
                       `names`: Option[Seq[String]] = None,
                       `referenceAllele`: String = "GGCA",
                       `alternateAlleles`: Seq[String] = Seq("G"),
                       `qual`: Option[Double] = None,
                       `filters`: Option[Seq[String]] = None,
                       `splitFromMultiAllelic`: Boolean = false,
                       `INFO_SpliceAI`: Seq[String] = Seq("G|KCNH1|0.1|0.1|0.1|0.1|-3|36|32|22"),
                       `INFO_OLD_MULTIALLELIC`: Option[String] = None,
                       `genotypes`: Seq[GENOTYPES] = Seq(GENOTYPES(sampleId = None)))
