package bio.ferlab.datalake.spark3.hive

import bio.ferlab.datalake.commons.config.Format.{DELTA, PARQUET}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, SimpleConfiguration, TableConf}
import bio.ferlab.datalake.spark3.testutils.WithTestConfig
import bio.ferlab.datalake.testutils.{CleanUpBeforeEach, SparkSpec, TestETLContext}
import org.apache.spark.sql.{AnalysisException, DataFrame}

class CreateTableAndViewSpec extends SparkSpec with WithTestConfig with CleanUpBeforeEach {

  import spark.implicits._

  val ds1: DatasetConf = DatasetConf("my_table_1", alias, "public/my_table_1", DELTA, OverWrite, table = TableConf("my_db", "my_table_1"), view = TableConf("my_other_db", "my_view_1"))
  val ds2: DatasetConf = DatasetConf("my_table_2", alias, "public/my_table_2", PARQUET, OverWrite, table = TableConf("my_db", "my_table_2"), view = TableConf("my_other_db", "my_view_2"))

  implicit val config: SimpleConfiguration = conf.copy(datalake = conf.datalake.copy(sources = List(ds1, ds2)))
  override val dsToClean: List[DatasetConf] = List(ds1, ds2)

  val df1: DataFrame = Seq(
    (1, "abc"),
    (2, "def")
  ).toDF("id", "val")

  val df2: DataFrame = Seq(
    (3, "ghi"),
    (4, "jkl")
  ).toDF("id", "val")

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Create files without creating tables
    df1.write.format(ds1.format.sparkFormat).save(ds1.location)
    df2.write.format(ds2.format.sparkFormat).save(ds2.location)
  }

  it should "create tables in the metastore from existing locations" in {
    // Tables can be read from location
    spark.read.format(ds1.format.sparkFormat).load(ds1.location).collect() should contain theSameElementsAs df1.collect()
    spark.read.format(ds2.format.sparkFormat).load(ds2.location).collect() should contain theSameElementsAs df2.collect()

    // But they can't be read using the metastore
    an[AnalysisException] shouldBe thrownBy(spark.table(ds1.table.get.fullName))
    an[AnalysisException] shouldBe thrownBy(spark.table(ds2.table.get.fullName))

    CreateTableAndView(TestETLContext(), Seq(ds1.id, ds2.id)).run()

    // Spark should now be able to read the tables using the metastore
    spark.table(ds1.table.get.fullName).collect() should contain theSameElementsAs df1.collect()
    spark.table(ds2.table.get.fullName).collect() should contain theSameElementsAs df2.collect()
  }

  it should "create views in the metastore from newly created tables" in {
    // Tables can be read from location
    spark.read.format(ds1.format.sparkFormat).load(ds1.location).collect() should contain theSameElementsAs df1.collect()
    spark.read.format(ds2.format.sparkFormat).load(ds2.location).collect() should contain theSameElementsAs df2.collect()

    // But views can't be read using the metastore
    an[AnalysisException] shouldBe thrownBy(spark.table(ds1.view.get.fullName))
    an[AnalysisException] shouldBe thrownBy(spark.table(ds2.view.get.fullName))

    CreateTableAndView(TestETLContext(), Seq(ds1.id, ds2.id)).run()

    // Spark should now be able to read the views using the metastore
    spark.table(ds1.view.get.fullName).collect() should contain theSameElementsAs df1.collect()
    spark.table(ds2.view.get.fullName).collect() should contain theSameElementsAs df2.collect()
  }

  it should "not fail when datasets are not found" in {
    noException shouldBe thrownBy {
      CreateTableAndView(TestETLContext(), Seq("ds1", "ds2")).run()
    }
  }
}
