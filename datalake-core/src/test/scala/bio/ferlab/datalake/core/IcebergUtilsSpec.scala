package bio.ferlab.datalake.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.catalog._
import org.apache.iceberg.{PartitionSpec, Schema, Table, types}
import org.apache.iceberg
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField.required
import org.apache.spark.sql.types._

class IcebergUtilsSpec extends AnyFlatSpec with GivenWhenThen with Matchers {

  implicit lazy val spark: SparkSession = SparkSession.builder()
    //.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    //.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    //.config("spark.sql.catalog.spark_catalog.type", "hive")
    //.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    //.config("spark.sql.catalog.local.type", "hadoop")
    //.config("spark.sql.catalog.local.warehouse", "spark-warehouse")
    //.enableHiveSupport()
    .master("local")
    .getOrCreate()


  "iceberg" should "read from an existing table" in {
    val schema: iceberg.Schema = new iceberg.Schema(
      required(1, "hotel_id", Types.LongType.get),
      required(2, "hotel_name", types.Types.StringType.get),
      required(3, "customer_id", types.Types.LongType.get),
      required(4, "arrival_date", Types.DateType.get),
      required(5, "departure_date", Types.DateType.get)
    )

    val spec: PartitionSpec = PartitionSpec.builderFor(schema).identity("hotel_id").build
    val id: TableIdentifier = TableIdentifier.parse("bookings.rome_hotels")

    import org.apache.iceberg.catalog.Catalog
    import org.apache.iceberg.hadoop.HadoopCatalog

    val catalog = new HadoopCatalog(spark.sparkContext.hadoopConfiguration, getClass.getResource(".").getFile)
    if(!catalog.tableExists(id)) catalog.createTable(id, schema, spec)
    spark.read
      .format("iceberg")
      .load("bookings.rome_hotels.snapshots")
      .show(truncate = false)
    //spark.read.format("iceberg").load(catalog.loadTable(id).location()).show(false)
    //catalog.loadTable(id).location()
  }

  /*
"iceberg" should "insert into an existing table" in {

  import spark.implicits._

  val df = Seq((1, "a"), (2, "b")).toDF("id", "data")

  df.write.format("iceberg")
    .mode("overwrite")
    .save("spark-warehouse/iceberg_table")

  spark.read.format("iceberg").load("spark-warehouse/iceberg_table").show(false)
  spark.sql("show tables").show(false)

}


"iceberg" should "create table" in {
  val sparkSchema = StructType(List(
    StructField("id", IntegerType, true),
    StructField("data", StringType, true)
  ))

  val icebergSchema = SparkSchemaUtil.convert(sparkSchema)

  val name = TableIdentifier.of("default","iceberg_table");

  val tables = new HadoopTables(spark.sessionState.newHadoopConf())
  if(!tables.exists("spark-warehouse/iceberg_table")) {
    tables.create(icebergSchema, "spark-warehouse/iceberg_table")
  }

}*/

}
