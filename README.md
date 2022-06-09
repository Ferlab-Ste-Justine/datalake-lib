# datalake-lib
Library built on top of Apache Spark to speed-up data lakes development.

## datalake-commons

Common classes between all modules.

## Version Matrix

The following table lists the versions supported of the main dependencies

| module | Spark Version | Delta Version | Glow Version | Scala version | Zio Version|
| ------ | ------------- | ------------- | ------------ | ------------- | ---------- |
| datalake-spark30 | `3.0.3` | `0.8.0` | `1.0.1` | `2.12` | `1.0.6`|
| datalake-spark31 | `3.1.2` | `1.0.0` | `1.0.1` | `2.12` | `1.0.6`|
| datalake-spark32 | `3.2.0` | `TBD` | `TBD` | `2.12` `2.13` | `TBD`|

## release

```
 sbt "publishSigned; sonatypeRelease"
```