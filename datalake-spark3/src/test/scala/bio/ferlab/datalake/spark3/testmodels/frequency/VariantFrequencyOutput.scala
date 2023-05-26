package bio.ferlab.datalake.spark3.testmodels.frequency



case class VariantFrequencyOutputByStudy(chromosome: String = "1",
                                         start: Long = 69897,
                                         reference: String = "T",
                                         alternate: String = "C",
                                         frequency_kf: GlobalFrequency = GlobalFrequency(total = Frequency(6, 6, 3, 3, 3)),
                                         frequency_by_study_id: Set[FrequencyByStudyId] = Set(FrequencyByStudyId(), FrequencyByStudyId(study_id = "S2", total = Frequency(2, 2, 1, 1, 1), participant_ids = null, transmissions = Set("AR")))
                                        )

case class VariantFrequencyOutputByStudyAffected(chromosome: String = "1",
                                                 start: Long = 69897,
                                                 reference: String = "T",
                                                 alternate: String = "C",
                                                 frequency_kf: GlobalFrequencyAffected = GlobalFrequencyAffected(total = Frequency(6, 6, 3, 3, 3), affected = Frequency(2, 2, 1, 1, 1), not_affected = Frequency(4, 4, 2, 2, 2)),
                                                 frequency_by_study_id: Set[FrequencyByStudyIdAffected] = Set(
                                                   FrequencyByStudyIdAffected(study_id = "S1",
                                                     total = Frequency(4, 4, 2, 2, 2),
                                                     not_affected = Frequency(2, 2, 1, 1, 1),
                                                     affected = Frequency(2, 2, 1, 1, 1)
                                                   ),
                                                   FrequencyByStudyIdAffected(study_id = "S2",
                                                     total = Frequency(2, 2, 1, 1, 1),
                                                     not_affected = Frequency(2, 2, 1, 1, 1),
                                                     affected = Frequency(0, 0, 0, 0, 0),
                                                     participant_ids = null,
                                                     transmissions = Set("AR")
                                                   ),

                                                 )
                                                )

case class GlobalFrequency(total: Frequency = Frequency())

case class GlobalFrequencyAffected(affected: Frequency = Frequency(0, 0, 0, 0, 0),
                                   not_affected: Frequency = Frequency(),
                                   total: Frequency = Frequency())

case class FrequencyByStudyId(study_id: String = "S1", total: Frequency = Frequency(), participant_ids: Set[String] = Set("P1", "P2"), transmissions: Set[String] = Set("AR", "AD"))

case class FrequencyByStudyIdAffected(study_id: String = "S1",
                                      affected: Frequency = Frequency(0, 0, 0, 0, 0),
                                      not_affected: Frequency = Frequency(),
                                      total: Frequency = Frequency(),
                                      participant_ids: Set[String] = Set("P1", "P2"), transmissions: Set[String] = Set("AR", "AD")
                                     )

case class Frequency(ac: Long = 4,
                     an: Long = 4,
                     pc: Long = 2,
                     pn: Long = 2,
                     hom: Long = 2)

case class SimpleOutput(chromosome: String = "1",
                        start: Long = 69897,
                        reference: String = "T",
                        alternate: String = "C",
                        ac: Long)
