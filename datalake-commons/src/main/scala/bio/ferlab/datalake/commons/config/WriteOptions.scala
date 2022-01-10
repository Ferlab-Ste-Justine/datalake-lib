package bio.ferlab.datalake.commons.config

/**
 * List of all write options supported
 */
object WriteOptions {
  final val CREATED_ON_COLUMN_NAME = "created_on_column"
  final val UPDATED_ON_COLUMN_NAME = "updated_on_column"
  final val VALID_FROM_COLUMN_NAME = "valid_from_column"
  final val VALID_TO_COLUMN_NAME = "valid_to_column"
  final val IS_CURRENT_COLUMN_NAME = "is_current_column"

  final val DEFAULT_CREATED_ON: String = "created_on"
  final val DEFAULT_UPDATED_ON: String = "updated_on"
  final val DEFAULT_VALID_FROM: String = "valid_from"
  final val DEFAULT_VALID_TO: String = "valid_to"
  final val DEFAULT_IS_CURRENT: String = "is_current"

  final val DEFAULT_CREATED_ON_PAIR: (String, String) = CREATED_ON_COLUMN_NAME -> DEFAULT_CREATED_ON
  final val DEFAULT_UPDATED_ON_PAIR: (String, String) = UPDATED_ON_COLUMN_NAME -> DEFAULT_UPDATED_ON
  final val DEFAULT_VALID_FROM_PAIR: (String, String) = VALID_FROM_COLUMN_NAME -> DEFAULT_VALID_FROM
  final val DEFAULT_VALID_TO_PAIR: (String, String) = VALID_TO_COLUMN_NAME -> DEFAULT_VALID_TO
  final val DEFAULT_IS_CURRENT_PAIR: (String, String) = VALID_TO_COLUMN_NAME -> DEFAULT_VALID_TO

  final val DEFAULT_OPTIONS: Map[String, String] =
    List(
      DEFAULT_VALID_TO_PAIR,
      DEFAULT_VALID_FROM_PAIR,
      DEFAULT_CREATED_ON_PAIR,
      DEFAULT_UPDATED_ON_PAIR,
      DEFAULT_IS_CURRENT_PAIR).toMap

}






