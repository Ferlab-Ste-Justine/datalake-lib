package bio.ferlab.datalake.spark3.elasticsearch



sealed trait AliasAction

case class AddAction(add: Map[String, String]) extends AliasAction


case class RemoveAction(remove: Map[String, String]) extends AliasAction

case class AliasActionsRequest(actions: Seq[AliasAction])
