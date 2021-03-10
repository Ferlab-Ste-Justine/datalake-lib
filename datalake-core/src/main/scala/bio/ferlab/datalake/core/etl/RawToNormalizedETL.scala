package bio.ferlab.datalake.core.etl

import bio.ferlab.datalake.core.config.Configuration
import bio.ferlab.datalake.core.transformation.Transformation
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class RawToNormalizedETL(val source: DataSource,
                         override val destination: DataSource,
                         val transformations: List[Transformation])
                        (override implicit val conf: Configuration) extends ETL(destination) {

  var processedFiles: List[String] = List()

  override def extract()(implicit spark: SparkSession): Map[DataSource, DataFrame] = {
    Map(source -> spark.read.format(source.format.sparkFormat).load(source.location))
  }

  /**
   * Takes a Map[DataSource, DataFrame] as input and apply a set of transformation to it to produce the ETL output.
   * It is recommended to not read any additional data but to use the extract() method instead to inject input data.
   *
   * @param data
   * @param spark an instance of SparkSession
   * @return
   */
  override def transform(data: Map[DataSource, DataFrame])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    //keep in memory which files are being processed
    processedFiles = data(source).withColumn("files", input_file_name())
      .select("files").as[String].collect().distinct.toList
    //apply list of transformations to the input data
    Transformation.applyTransformations(data(source), transformations)
  }

  /**
   * Loads the output data into a persistent storage.
   * The output destination can be any of: object store, database or flat files...
   *
   * @param data  output data produced by the transform method.
   * @param spark an instance of SparkSession
   */
  override def load(data: DataFrame)(implicit spark: SparkSession): Unit = {
    //TODO replace with DeltaUtils.upsert()
    data
      .write
      .format(destination.format.sparkFormat)
      .mode(SaveMode.Overwrite)
      .option("path", destination.location)
      .saveAsTable(s"${destination.database}.${destination.name}")
  }


  /**
   * OPTIONAL - Contains all actions needed to be done in order to make the data available to users
   * like creating a view with the data.
   * @param spark an instance of SparkSession
   */
  override def publish()(implicit spark: SparkSession): Unit = {
    processedFiles.foreach(file =>
      //TODO move to archive folder
      println(s"Moving to /raw/archive: $file")
    )
  }
}

