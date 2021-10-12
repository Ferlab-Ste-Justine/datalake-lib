/**
 * Copyright (C) 2021 Ferlab Ste-Justine contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

lazy val scala212 = "2.12.14"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

scalacOptions ++= Seq("-deprecation", "-Ypartial-unification")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark3Version = "3.1.2"
val spark2Version = "2.4.8"
val deltaCoreVersion = "1.0.0"
val catsVersion = "2.6.1"
val scalatestVersion = "3.2.9"
val glowVersion = "1.0.1"
val zioVersion = "1.0.6"
val pureconfigVersion = "0.16.0"
val elasticsearchVersion = "7.12.0"

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
  //releaseStepCommandAndRemaining("+publishSigned"),
  //releaseStepCommand(";project `datalake-spark3`; sonatypeBundleRelease"),
  //releaseStepCommand(";project `datalake-spark2`; sonatypeBundleRelease"),
  //releaseStepCommand("sonatypeBundleClean"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / releaseProcess := releaseSteps
ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
ThisBuild / releasePublishArtifactsAction := PgpKeys.publishSigned.value
ThisBuild / fork := true
ThisBuild / versionScheme := Some("semver-spec")

lazy val `datalake-spark3` = (project in file("datalake-spark3"))
  .settings(
    scalaVersion := scala212,
    libraryDependencies += "org.apache.spark"      %% "spark-core"             % spark3Version % Provided,
    libraryDependencies += "org.apache.spark"      %% "spark-sql"              % spark3Version % Provided,
    libraryDependencies += "io.delta"              %% "delta-core"             % deltaCoreVersion % Provided,
    libraryDependencies += "org.elasticsearch"     %% "elasticsearch-spark-30" % elasticsearchVersion % Provided,
    libraryDependencies += "com.github.pureconfig" %% "pureconfig"             % pureconfigVersion,
    libraryDependencies += "com.github.pureconfig" %% "pureconfig-enum"        % pureconfigVersion,
    libraryDependencies += "org.typelevel"         %% "cats-core"              % catsVersion,
    libraryDependencies += "org.scalatest"         %% "scalatest"              % scalatestVersion % Test,
    libraryDependencies += "org.apache.spark"      %% "spark-hive"             % spark3Version % Test,
    libraryDependencies += "io.projectglow"        %% "glow-spark3"            % glowVersion % Provided exclude ("org.apache.hadoop", "hadoop-client"),
    libraryDependencies += "dev.zio"               %% "zio-config-typesafe"    % zioVersion,
    libraryDependencies += "dev.zio"               %% "zio-config"             % zioVersion,
    libraryDependencies += "dev.zio"               %% "zio-config-magnolia"    % zioVersion,
    dependencyOverrides ++= Seq(
      "org.apache.commons"    % "commons-lang3"           % "3.9",
      "org.antlr"    % "antlr4"           % "4.8",
      "org.antlr"    % "antlr4-tool"      % "4.8",
      "org.antlr"    % "antlr4-runtime"   % "4.8"
    )
  )

lazy val `datalake-spark2` = (project in file("datalake-spark2"))
  .settings(
    scalaVersion := scala211,
    libraryDependencies += "org.apache.spark"          %% "spark-sql"              % spark2Version % Provided,
    libraryDependencies += "org.elasticsearch"         %% "elasticsearch-spark-20" % "7.9.1" % Provided,
    libraryDependencies += "org.scalatest"             %% "scalatest"              % scalatestVersion % Test,
    libraryDependencies += "org.apache.spark"          %% "spark-hive"             % spark2Version % Test,
    libraryDependencies += "org.apache.httpcomponents" %  "httpclient"             % "4.5.13",
    dependencyOverrides += "org.apache.commons"        % "commons-lang3"           % "3.9"
  )
