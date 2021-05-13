/**
 * Copyright (C) 2021 Ferlab Ste-Justine contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

lazy val scala212 = "2.12.13"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

scalacOptions ++= Seq("-deprecation", "-Ypartial-unification")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark3Version = "3.0.2"
val spark2Version = "2.4.7"
val deltaCoreVersion = "0.8.0"
val typesafeVersion = "1.4.1"
val catsVersion = "2.2.0"
val scalatestVersion = "3.2.0"
val glowVersion = "0.6.0"
val elasticsearch_spark_version = "7.9.1"

import ReleaseTransformations._
val releaseSteps = Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  //runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  //releaseStepCommand("publishSigned"),
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("project datalake-spark3;sonatypeBundleRelease"),
  releaseStepCommand("project datalake-spark2;sonatypeBundleRelease"),
  releaseStepCommand("sonatypeBundleClean"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / releaseProcess := releaseSteps
ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
ThisBuild / releasePublishArtifactsAction := PgpKeys.publishSigned.value
ThisBuild / fork := true

lazy val `datalake-spark3` = (project in file("datalake-spark3"))
  .settings(
    scalaVersion := scala212,
    libraryDependencies += "org.apache.spark"      %% "spark-core" % spark3Version % Provided,
    libraryDependencies += "org.apache.spark"      %% "spark-sql"  % spark3Version % Provided,
    libraryDependencies += "io.delta"              %% "delta-core" % deltaCoreVersion % Provided,
    libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.14.1",
    libraryDependencies += "org.typelevel"         %% "cats-core"  % catsVersion,
    libraryDependencies += "org.scalatest"         %% "scalatest"  % scalatestVersion % Test,
    libraryDependencies += "io.projectglow"        %% "glow-spark3"% glowVersion  exclude ("org.apache.hadoop", "hadoop-client"))

lazy val `datalake-spark2` = (project in file("datalake-spark2"))
  .settings(
    scalaVersion := scala211,
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % spark2Version % Provided,
    libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % elasticsearch_spark_version % Provided
  )
