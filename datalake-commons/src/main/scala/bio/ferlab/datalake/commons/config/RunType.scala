package bio.ferlab.datalake.commons.config

import enum.Enum

sealed trait RunType

/**
 * List of all LoadTypes supported
 */
object RunType {
  case object FIRST_LOAD extends RunType
  case object INCREMENTAL_LOAD extends RunType
  case object SAMPLE_LOAD extends RunType

  implicit val EnumInstance: Enum[RunType] = Enum.derived[RunType]
}
