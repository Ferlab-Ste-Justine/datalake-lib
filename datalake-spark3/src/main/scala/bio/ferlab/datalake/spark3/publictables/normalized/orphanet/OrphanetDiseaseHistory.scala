package bio.ferlab.datalake.spark3.publictables.normalized.orphanet

case class OrphanetDiseaseHistory(disorder_id: Long,
                                  orpha_code: Long,
                                  expert_link: String,
                                  name: String,
                                  disorder_type_id: Long,
                                  disorder_type_name: String,
                                  disorder_group_id: Long,
                                  disorder_group_name: String,
                                  average_age_of_onset: List[String],
                                  average_age_of_death: List[String],
                                  type_of_inheritance: List[String])
