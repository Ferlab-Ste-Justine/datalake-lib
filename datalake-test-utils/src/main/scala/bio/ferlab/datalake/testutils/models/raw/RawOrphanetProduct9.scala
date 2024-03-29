package bio.ferlab.datalake.testutils.models.raw

case class RawOrphanetProduct9(disorder_id: Long = 2,
                               orpha_code: Long = 58,
                               expert_link: String = "http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&Expert=58",
                               name: String = "Alexander disease",
                               disorder_type_id: Long = 21394,
                               disorder_type_name: String = "Disease",
                               disorder_group_id: Long = 36547,
                               disorder_group_name: String = "Disease",
                               average_age_of_onset: List[String] = List("All ages"),
                               average_age_of_death: List[String] = List("any age"),
                               type_of_inheritance: List[String] = List("Autosomal dominant"))
