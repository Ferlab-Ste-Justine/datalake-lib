package bio.ferlab.datalake.spark3.testmodels.frequency


case class VariantFrequencyOutputByStudy(chromosome: String = "1",
                                         start: Long = 69897,
                                         reference: String = "T",
                                         alternate: String = "C",
                                         frequency_kf: GlobalFrequency = GlobalFrequency(total = Frequency()),
                                         frequency_by_study_id: Set[FrequencyByStudyId] = Set(
                                           FrequencyByStudyId(),
                                           FrequencyByStudyId(study_id = "S2", total = Frequency(ac = 2, pc = 1, hom = 1, an = 4, pn = 2, af = 0.5, pf = 0.5), participant_ids = null, transmissions = Set("AR"), study_code = "STUDY_CODE_2"))
                                        )

case class VariantFrequencyOutputByStudyAffected(chromosome: String = "1",
                                                 start: Long = 69897,
                                                 reference: String = "T",
                                                 alternate: String = "C",
                                                 frequency_kf: GlobalFrequencyAffected = GlobalFrequencyAffected(
                                                   total = Frequency(ac = 5, pc = 3, hom = 2, an = 8, pn = 4, af = 0.625, pf = 0.75),
                                                   affected = Frequency(ac = 1, pc = 1, hom = 0, an = 2, pn = 1, af = 0.5, pf = 1.0),
                                                   not_affected = Frequency(ac = 4, pc = 2, hom = 2, an = 6, pn = 3, af = 4d / 6d, pf = 2d / 3d)),
                                                 frequency_by_study_id: Set[FrequencyByStudyIdAffected] = Set(
                                                   FrequencyByStudyIdAffected(
                                                     study_id = "S1",
                                                     total = Frequency(ac = 3, pc = 2, hom = 1, an = 4, pn = 2, af = 0.75, pf = 1.0),
                                                     not_affected = Frequency(ac = 2, pc = 1, hom = 1, an = 2, pn = 1, af = 1.0, pf = 1.0),
                                                     affected = Frequency(ac = 1, pc = 1, hom = 0, an = 2, pn = 1, af = 0.5, pf = 1.0)
                                                   ),
                                                   FrequencyByStudyIdAffected(study_id = "S2",
                                                     total = Frequency(ac = 2, pc = 1, hom = 1, an = 4, pn = 2, af = 0.5, pf = 0.5),
                                                     not_affected = Frequency(ac = 2, pc = 1, hom = 1, an = 4, pn = 2, af = 0.5, pf = 0.5),
                                                     affected = Frequency(0, 0, 0, 0, 0, 0.0, 0.0),
                                                     participant_ids = null,
                                                     transmissions = Set("AR"),
                                                     study_code = "STUDY_CODE_2"
                                                   ),

                                                 )
                                                )

case class GlobalFrequency(total: Frequency = Frequency(), zygosities: Set[String] = Set("HOM", "HET"))

case class GlobalFrequencyAffected(affected: Frequency = Frequency(0, 0, 0, 0, 0),
                                   not_affected: Frequency = Frequency(),
                                   total: Frequency = Frequency(),
                                   zygosities: Set[String] = Set("HOM", "HET")
                                  )

case class FrequencyByStudyId(study_id: String = "S1", total: Frequency = Frequency(ac = 3, pc = 2, hom = 1, an = 4, pn = 2, af = 0.75, pf = 1.0), participant_ids: Set[String] = Set("P1", "P2"), transmissions: Set[String] = Set("AR", "AD"), study_code: String = "STUDY_CODE_1")

case class FrequencyByStudyIdAffected(study_id: String = "S1",
                                      affected: Frequency = Frequency(0, 0, 0, 0, 0),
                                      not_affected: Frequency = Frequency(),
                                      total: Frequency = Frequency(),
                                      participant_ids: Set[String] = Set("P1", "P2"), transmissions: Set[String] = Set("AR", "AD"),
                                      study_code: String = "STUDY_CODE_1"
                                     )

case class Frequency(ac: Long = 5,
                     an: Long = 8,
                     pc: Long = 3,
                     pn: Long = 4,
                     hom: Long = 2,
                     af: Double = 0.625, // 5 / 8,
                     pf: Double = 0.75
                    )

case class SimpleOutput(chromosome: String = "1",
                        start: Long = 69897,
                        reference: String = "T",
                        alternate: String = "C",
                        ac: Long)
