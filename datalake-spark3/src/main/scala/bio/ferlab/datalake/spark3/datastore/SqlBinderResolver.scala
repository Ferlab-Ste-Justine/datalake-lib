package bio.ferlab.datalake.spark3.datastore

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, Format}
import bio.ferlab.datalake.spark3.hive.HiveFieldComment
import org.apache.spark.sql.SparkSession

object SqlBinderResolver {

  def drop(implicit spark: SparkSession, conf: Configuration): PartialFunction[Format, DatasetConf => Unit] = {
    case _ => ds => ds.table.foreach(t => HiveSqlBinder.drop(ds.location, t.database, t.name, conf.getStorage(ds.storageid).filesystem))
  }

  def setComment(implicit spark: SparkSession, conf: Configuration): PartialFunction[Format, (DatasetConf, HiveFieldComment) => Unit] = {
    case _ => (ds, hfc) =>
      ds.table.foreach(t => HiveSqlBinder.setComment(hfc.col_name, hfc.data_type, hfc.comment, t.database, t.name))
  }

}
