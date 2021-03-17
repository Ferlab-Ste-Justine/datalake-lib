package bio.ferlab.datalake.core.loader

/**
 * List of all LoadTypes supported by Sparkles
 */
object LoadTypes extends Enumeration {
  type LoadType = Value
  val Compact, OverWrite, Insert, Upsert, Scd1, Scd2 = Value
}
