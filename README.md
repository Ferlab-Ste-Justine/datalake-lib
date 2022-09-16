# datalake-lib
Library built on top of Apache Spark to speed-up data lakes development.

## Core concepts

### Configuration file

1. Define all the datasets your ETLs need to interact with.
```scala
val raw = "raw"
val curated = "curated"
val config = SimpleConfiguration(
  datalake = DatalakeConf(
    sources = List(
      DatasetConf("raw_data1"    , raw    , "/data1", JSON , OverWrite),
      DatasetConf("raw_data2"    , raw    , "/data2", JSON , OverWrite),
      DatasetConf("curated_data1", curated, "/data1", DELTA, OverWrite),
      DatasetConf("curated_data2", curated, "/data2", DELTA, OverWrite)
    ),
    sparkconf = Map(
      "spark.hadoop.fs.s3a.endpoint" -> "https://example.com"
    )
  )
)
```

2. Generate a specific configuration for each environments
```scala
val localStorages = List(
  StorageConf(raw    , "~/raw"    , LOCAL),
  StorageConf(curated, "~/curated", LOCAL)
)

val devStorages = List(
  StorageConf(raw    , "s3a://dev-raw"    , S3),
  StorageConf(curated, "s3a://dev-curated", S3)
)

val prodStorages = List(
  StorageConf(raw    , "s3a://prod-raw"    , S3),
  StorageConf(curated, "s3a://prod-curated", S3)
)
val localConf = config.copy(config.datalake.copy(storages = localStorages))
val devConf = config.copy(config.datalake.copy(storages = devStorages))
val prodConf = config.copy(config.datalake.copy(storages = devStorages))
```
3. Generate a configuration file as HOCON format

```scala
ConfigurationWriter.writeTo("src/test/resources/config/local.conf", localConf)
ConfigurationWriter.writeTo("src/main/resources/config/dev.conf", devConf)
ConfigurationWriter.writeTo("src/main/resources/config/prod.conf", prodConf)
```

4. Load the configuration file and make it available in your unit tests or ETLs

```scala
implicit val conf = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/local.conf")
```

### Define your own configuration case class

You can slo define your own case class, if you want for example extend the datalake configuraion.

1. Define your case class, it must extends `ConfigurationWrapper` :

```scala
case class ExtraConf(extraOption: String, datalake: DatalakeConf) extends ConfigurationWrapper(datalake)
```

2. For writing your configuration, use ConfigurationWriter

```scala   
ConfigurationWriter.writeTo("src/test/resources/config/local.conf", localConf)
```

3. For loading your configuration 

```scala
implicit val conf = ConfigurationLoader.loadFromResources[ExtraConf]("config/local.conf")
```

### ETL class

An ETL defines these main functions on top of an entry point `run()`:

| method    | default behavior                                                                           |
|-----------|--------------------------------------------------------------------------------------------|
| reset     | Delete all the files and metadata from the mainDestination of the ETL                      |
| extract   | **Not implemented**                                                                        |
| sampling  | Takes 5% of the data from each sources returned byt the function extract()                 |
| transform | **Not implemented**                                                                        |
|load| Persist all DataFrames returned by the function transform() using the default LoadResolver |
|publish| does nothing                                                                               |


These are called in order by the function `run()` to which you can passe a list of `RunStep` which dictate the steps that are going to be effectively run or skiped at runtime.
For instance, assuming we instantiated an ETL called `job`:
- `job.run(RunStep.initial_load)` will call `reset()`, skip `sampling()` and run all remaining steps
- `job.run(RunStep.default_load)` will skip both `reset()` and `sampling()` and run all remaining steps
- `job.run(RunStep.allSteps)` will all steps

It is also possible to run only certain steps on demand, for more details about this see `bio.ferlab.datalake.commons.config.RunStep`

## datalake-commons

Common classes between all modules.

## Version Matrix

The following table lists the versions supported of the main dependencies

| module | Spark Version | Delta Version | Glow Version | Scala version | Zio Version |
| ------ |---------------|---------------|--------------| ------------- |-------------|
| datalake-spark3 | `3.0.3`       | `0.8.0`       | `1.0.1`      | `2.12` | `1.0.6`     |
| datalake-spark3 | `3.1.3`       | `1.1.0`       | `1.0.1`      | `2.12` | `1.0.6`     |
| datalake-spark3 | `3.2.2`       | `1.2.0`       | `1.2.1`      | `2.12` `2.13` | `1.0.6`     |

## release

```
 sbt "publishSigned; sonatypeRelease"
```