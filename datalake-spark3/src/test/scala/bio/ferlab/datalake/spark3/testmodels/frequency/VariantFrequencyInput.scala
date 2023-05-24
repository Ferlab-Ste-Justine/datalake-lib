package bio.ferlab.datalake.spark3.testmodels.frequency

case class VariantFrequencyInput(chromosome: String = "1",
                                 start: Long = 1000,
                                 reference: String = "A",
                                 alternate: String = "T",
                                 calls: Seq[Int] = Seq(1, 1),
                                 affected_status: Boolean = false,
                                 zygosity: String = "HOM",
                                 study_id: String = "S1",
                                 ethnicity: Option[String] = None,
                                 patient_id: String = "P1"
                                )

case class VariantFrequencyOutputByStudy(chromosome: String = "1",
                                         start: Long = 1000,
                                         reference: String = "A",
                                         alternate: String = "T",
                                         frequency_kf: GlobalFrequency = GlobalFrequency(total = Frequency(6, 6, 3, 3, 3)),
                                         frequency_by_study_id: Set[FrequencyByStudyId] = Set(FrequencyByStudyId(), FrequencyByStudyId(study_id = "S2", total = Frequency(2, 2, 1, 1, 1)))
                                        )

case class VariantFrequencyOutputByStudyAffected(chromosome: String = "1",
                                                 start: Long = 1000,
                                                 reference: String = "A",
                                                 alternate: String = "T",
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
                                                     affected = Frequency(0, 0, 0, 0, 0))
                                                 )
                                                )

case class GlobalFrequency(total: Frequency = Frequency())

case class GlobalFrequencyAffected(affected: Frequency = Frequency(0, 0, 0, 0, 0),
                                   not_affected: Frequency = Frequency(),
                                   total: Frequency = Frequency())

case class FrequencyByStudyId(study_id: String = "S1", total: Frequency = Frequency())

case class FrequencyByStudyIdAffected(study_id: String = "S1",
                                      affected: Frequency = Frequency(0, 0, 0, 0, 0),
                                      not_affected: Frequency = Frequency(),
                                      total: Frequency = Frequency())

case class Frequency(ac: Long = 4,
                     an: Long = 4,
                     pc: Long = 2,
                     pn: Long = 2,
                     hom: Long = 2)

case class SimpleOutput(chromosome: String = "1",
                        start: Long = 1000,
                        reference: String = "A",
                        alternate: String = "T",
                        ac: Long)
