/**
 * Copyright (C) 2021 Ferlab Ste-Justine contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

lazy val scala212 = "2.12.18"
lazy val supportedScalaVersions = List(scala212)

scalacOptions ++= Seq("-deprecation", "-Ypartial-unification")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark3Version = "3.3.1"
val catsVersion = "2.7.0"
val scalatestVersion = "3.2.12"
val pureconfigVersion = "0.17.2"
val elasticsearchVersion = "7.15.0"
val deltaVersion = "2.1.1"
val glowVersion = "1.2.1"

updateOptions := updateOptions.value.withGigahorse(false)

lazy val `datalake-commons` = (project in file("datalake-commons"))
  .settings(
    scalaVersion := scala212,
    libraryDependencies += "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,
    libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0",
    libraryDependencies += "org.typelevel" %% "cats-core" % catsVersion,
    libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion,
    libraryDependencies += "io.projectglow" %% "glow-spark3" % glowVersion % Provided exclude("org.apache.hadoop", "hadoop-client"),
    libraryDependencies += "com.outr" %% "hasher" % "1.2.2",
    libraryDependencies += "org.apache.spark" %% "spark-core" % spark3Version % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % spark3Version % Provided,
    dependencyOverrides ++= Seq(
      "org.apache.commons" % "commons-lang3" % "3.9",
      "org.antlr" % "antlr4" % "4.8",
      "org.antlr" % "antlr4-tool" % "4.8",
      "org.antlr" % "antlr4-runtime" % "4.8"
    )
  )

lazy val `datalake-test-utils` = (project in file("datalake-test-utils"))
  .settings(
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % spark3Version % Provided,
      "org.apache.spark" %% "spark-sql" % spark3Version % Provided,
      "io.delta" %% "delta-core" % deltaVersion % Provided,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val `datalake-spark3` = (project in file("datalake-spark3"))
  .settings(
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % spark3Version % Provided,
      "org.apache.spark" %% "spark-sql" % spark3Version % Provided,
      "io.delta" %% "delta-core" % deltaVersion % Provided,
      "org.elasticsearch" %% "elasticsearch-spark-30" % elasticsearchVersion % Provided,
      "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "io.projectglow" %% "glow-spark3" % glowVersion % Provided exclude("org.apache.hadoop", "hadoop-client"),
      "com.microsoft.sqlserver" % "mssql-jdbc" % "8.4.1.jre8" % Provided,
      "com.microsoft.aad" % "adal4j" % "0.0.2" % Provided,
      "com.microsoft.azure" % "spark-mssql-connector_2.12" % "1.1.0" % Provided,
      "com.lihaoyi"%% "mainargs" % "0.5.0",
      //Use by ElasticsearchClient
      "com.softwaremill.sttp.client3" %% "core" % "3.8.15",
      "com.softwaremill.sttp.client3" %% "json4s" % "3.8.15" exclude("org.json4s", "json4s-core_2.12"), //Exclusion because json4s is used in spark
      "com.softwaremill.sttp.client3" %% "slf4j-backend" % "3.8.15",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.17" % Test,
      "com.dimafeng" %% "testcontainers-scala-elasticsearch" % "0.40.17" % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.apache.spark" %% "spark-hive" % spark3Version % Test,

    ),
    dependencyOverrides ++= Seq(
      "org.apache.commons" % "commons-lang3" % "3.9",
      "org.antlr" % "antlr4-runtime" % "4.8",
      "org.antlr" % "antlr4-tool" % "4.7.1",
    ),
  )
  .dependsOn(`datalake-commons`)
  .dependsOn(`datalake-test-utils` % "test->compile")



Test / fork := true
