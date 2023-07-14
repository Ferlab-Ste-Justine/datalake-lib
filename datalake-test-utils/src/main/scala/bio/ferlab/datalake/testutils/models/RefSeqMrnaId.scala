package bio.ferlab.datalake.testutils.models

case class RefSeqMrnaIdInput(id: String = "1", annotation: Option[RefSeqAnnotation] = Some(RefSeqAnnotation()))

case class RefSeqAnnotation(RefSeq: Option[String] = Some("123&456"))


case class RefSeqMrnaIdInputWithoutRefSeq(id: String = "1", annotation: Option[RefSeqAnnotationWithoutRefSeq] = Some(RefSeqAnnotationWithoutRefSeq()))

case class RefSeqAnnotationWithoutRefSeq(another: Option[String] = Some("test"))

case class RefSeqMrnaIdInputWithoutAnnotation(id: String = "1", another: String = "test")