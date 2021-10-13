package bio.ferlab.datalake.commons.config

sealed trait WriteOption {
  val value: String
}

/**
 * List of all write options supported
 */
object WriteOptions {
  case object CREATED_ON_COLUMN_NAME extends WriteOption { val value = "created_on_column" }
  case object UPDATED_ON_COLUMN_NAME extends WriteOption { val value = "updated_on_column" }
  case object VALID_FROM_COLUMN_NAME extends WriteOption { val value = "valid_from_column" }
  case object VALID_TO_COLUMN_NAME extends WriteOption { val value = "valid_to_column" }

  final val DEFAULT_CREATED_ON: (String, String) = CREATED_ON_COLUMN_NAME.value -> "created_on"
  final val DEFAULT_UPDATED_ON: (String, String) = UPDATED_ON_COLUMN_NAME.value -> "updated_on"
  final val DEFAULT_VALID_FROM: (String, String) = VALID_FROM_COLUMN_NAME.value -> "valid_from"
  final val DEFAULT_VALID_TO: (String, String) = VALID_TO_COLUMN_NAME.value -> "valid_to"

  final val DEFAULT_OPTIONS: Map[String, String] =
    List(
      DEFAULT_VALID_TO,
      DEFAULT_VALID_FROM,
      DEFAULT_CREATED_ON,
      DEFAULT_UPDATED_ON).toMap

}






