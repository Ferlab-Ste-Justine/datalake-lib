# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Compile all modules
sbt compile

# Run all tests
sbt test

# Run tests for a specific module
sbt "datalake-spark3/test"
sbt "datalake-commons/test"
sbt "datalake-test-utils/test"

# Run a single test class
sbt "datalake-spark3/testOnly bio.ferlab.datalake.spark3.etl.v4.SingleETLSpec"

# Run a single test by name pattern
sbt "datalake-spark3/testOnly *SingleETLSpec"

# Publish locally (for use in dependent projects)
sbt publishLocal

# Publish locally with a specific version
VERSION=14.14.2-SNAPSHOT sbt publishLocal

# Release to Sonatype
sbt "publishSigned; sonatypeRelease"
```

## Project Structure

This is an sbt multi-module project with three modules:

- **`datalake-commons`** — Core configuration classes, no Spark dependency at compile time. Contains `Configuration`, `DatasetConf`, `StorageConf`, `LoadType`, `RunStep`, and config loading/writing utilities.
- **`datalake-spark3`** — Main library built on Spark 3.5 and Delta 3.1. Contains ETL abstractions, loaders, transformations, and genomics utilities. Depends on `datalake-commons`.
- **`datalake-test-utils`** — Test helpers including `SparkSpec`, `WithSparkSession`, cleanup traits, and typed fixture models for genomics data. Depends on `datalake-commons`.

Scala 2.12 only. Tests fork a JVM (`Test / fork := true`).

## Architecture

### Configuration System

Configuration is loaded from HOCON files using pureconfig. The base trait is `Configuration` (in `datalake-commons`), which holds:
- `storages: List[StorageConf]` — named storage backends (LOCAL, S3, GCS, etc.)
- `sources: List[DatasetConf]` — all datasets the ETLs interact with, each with a unique `id`, a storage alias, a relative path, a `Format`, and a `LoadType`

`SimpleConfiguration` and `DatalakeConf` are the standard concrete implementations. Projects can define their own by extending `ConfigurationWrapper`.

Use `ConfigurationLoader.loadFromResources[MyConf]("config/local.conf")` to load, and `ConfigurationWriter.writeTo(path, conf)` to generate HOCON files.

### ETL Abstraction (versioned)

There are three active ETL versions in `datalake-spark3/etl/`:

- **v2** — Legacy, avoid for new code.
- **v3** — Previous generation, `ETL[C]` parameterized only on config type.
- **v4** — Current generation (prefer this). `ETL[T, C]` is parameterized on both a data-change tracking type `T` (e.g. `LocalDateTime`, `String`) and config type `C`. The `T` parameter enables incremental loads by tracking `lastRunValue`/`currentRunValue`.

Key v4 ETL hierarchy:
- `ETL[T, C]` — base abstract class; implement `extract`, `transform`, `load`
- `SingleETL[T, C]` — simplification for ETLs with one output; implement `transformSingle` instead of `transform`
- `ETLP[T, C]` — extends `SingleETL`; adds automatic `publish()` that creates Hive views and updates table comments from a documentation path
- `TransformationsETL[T, C]` — concrete class; takes a source `DatasetConf` and a `List[Transformation]`, no subclassing needed

All ETLs receive an `ETLContext[T, C]` which bundles the `SparkSession`, config, and `runSteps`.

The ETL lifecycle: `(reset)` → `extract` → `(sample)` → `transform` → `load` → `publish`. Steps are controlled by `RunStep`:
- `RunStep.default_load` — extract, transform, load, publish (most common)
- `RunStep.initial_load` — reset + default_load
- `RunStep.allSteps` — includes sampling

### Load Types and Loaders

`LoadType` (in `datalake-commons`) defines write strategies: `OverWrite`, `OverWritePartition`, `OverWritePartitionDynamic`, `Insert`, `Upsert`, `Scd1`, `Scd2`, `Compact`, `Read`.

`LoadResolver` (in `datalake-spark3/loader/`) dispatches `(Format, LoadType)` pairs to the appropriate `Loader` implementation:
- `DeltaLoader` — full support for all load types including SCD1/SCD2 merge patterns
- `GenericLoader` — fallback for Parquet, JSON, CSV, etc.
- `JdbcLoader` / `SqlServerLoader` — JDBC targets
- `ElasticsearchLoader` — ES indexing
- `ExcelLoader` — Excel read/write
- `VcfLoader` — genomic VCF files via Glow

### Transformations

`Transformation` (in `datalake-spark3/transformation/`) is a trait with a single `transform(df: DataFrame): DataFrame` method. Many built-in implementations exist (e.g., `Cast`, `Rename`, `Drop`, `RegexExtract`, `ToDate`, `NormalizeColumnName`). They are composed as a `List[Transformation]` and applied via `Transformation.applyTransformations`.

### Testing

Tests extend `SparkSpec` (from `datalake-test-utils`), which mixes in `AnyFlatSpec`, `Matchers`, and `WithSparkSession`. Use `CleanUpBeforeAll` or `CleanUpBeforeEach` to reset file-system state between tests. Typed fixture models (raw/normalized/enriched) live in `datalake-test-utils/models/`.
