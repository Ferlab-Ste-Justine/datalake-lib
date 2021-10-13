package bio.ferlab.datalake.commons.config

import enum.Enum

sealed trait LoadType

/**
 * List of all LoadTypes supported
 */
object LoadType {
  case object Read extends LoadType
  case object Compact extends LoadType
  case object OverWrite extends LoadType
  case object Insert extends LoadType
  case object Upsert extends LoadType
  case object Scd1 extends LoadType
  case object Scd2 extends LoadType

  implicit val EnumInstance: Enum[LoadType] = Enum.derived[LoadType]
}
