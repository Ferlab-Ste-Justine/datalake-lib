/**
 * Copyright (C) 2021 Ferlab Ste-Justine contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

lazy val scala212 = "2.12.14"
lazy val supportedScalaVersions = List(scala212)

scalacOptions ++= Seq("-deprecation", "-Ypartial-unification")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark30Version = "3.0.3"
val spark31Version = "3.1.2"
val catsVersion = "2.6.1"
val scalatestVersion = "3.2.9"
val zioVersion = "1.0.6"
val pureconfigVersion = "0.16.0"
val elasticsearchVersion = "7.15.0"

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

lazy val `datalake-commons` = (project in file("datalake-commons"))
  .settings(
    scalaVersion := scala212,
    libraryDependencies += "com.github.pureconfig" %% "pureconfig"             % pureconfigVersion,
    libraryDependencies += "com.github.pureconfig" %% "pureconfig-enum"        % pureconfigVersion,
    libraryDependencies += "org.typelevel"         %% "cats-core"              % "2.6.1",
    libraryDependencies += "org.scalatest"         %% "scalatest"              % scalatestVersion,
    libraryDependencies += "io.projectglow"        %% "glow-spark3"            % "1.0.1" % Provided exclude ("org.apache.hadoop", "hadoop-client"),
    libraryDependencies += "dev.zio"               %% "zio-config-typesafe"    % zioVersion,
    libraryDependencies += "dev.zio"               %% "zio-config"             % zioVersion,
    libraryDependencies += "dev.zio"               %% "zio-config-magnolia"    % zioVersion,
    libraryDependencies += "com.outr"              %% "hasher"                 % "1.2.2",
    dependencyOverrides ++= Seq(
      "org.apache.commons"    % "commons-lang3"           % "3.9",
      "org.antlr"    % "antlr4"           % "4.8",
      "org.antlr"    % "antlr4-tool"      % "4.8",
      "org.antlr"    % "antlr4-runtime"   % "4.8"
    )
  )

lazy val `datalake-spark3` = (project in file("datalake-spark3"))
  .settings(
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.apache.spark"        %% "spark-core"                     % spark31Version       % Provided,
      "org.apache.spark"        %% "spark-sql"                      % spark31Version       % Provided,
      "io.delta"                %% "delta-core"                     % "1.0.0"              % Provided,
      "org.elasticsearch"       %% "elasticsearch-spark-30"         % elasticsearchVersion % Provided,
      "com.github.pureconfig"   %% "pureconfig"                     % pureconfigVersion,
      "com.github.pureconfig"   %% "pureconfig-enum"                % pureconfigVersion,
      "org.typelevel"           %% "cats-core"                      % catsVersion,
      "io.projectglow"          %% "glow-spark3"                    % "1.1.0"              % Provided exclude ("org.apache.hadoop", "hadoop-client"),
      "dev.zio"                 %% "zio-config-typesafe"            % zioVersion,
      "dev.zio"                 %% "zio-config"                     % zioVersion,
      "dev.zio"                 %% "zio-config-magnolia"            % zioVersion,
      "software.amazon.awssdk"  %  "s3"                             % "2.17.71"            % Provided,
      "com.microsoft.sqlserver" %  "mssql-jdbc"                     % "8.4.1.jre8"         % Provided,
      "com.microsoft.aad"       %  "adal4j"                         % "0.0.2"              % Provided,
      "com.microsoft.azure"     %  "spark-mssql-connector_2.12"     % "1.1.0"              % Provided,
      "com.dimafeng"            %% "testcontainers-scala-scalatest" % "0.38.8"             % Test,
      "org.testcontainers"      %  "localstack"                     % "1.15.2"             % Test,
      "org.scalatest"           %% "scalatest"                      % scalatestVersion,
      "org.apache.spark"        %% "spark-hive"                     % spark31Version       % Test
),
    dependencyOverrides ++= Seq(
      "org.apache.commons"     %  "commons-lang3"                   % "3.9",
      "org.antlr"              % "antlr4-runtime"                   % "4.8",
      "org.antlr"              % "antlr4-tool"                      % "4.7.1",
    ),
  )
  .dependsOn(`datalake-commons`)

Test / fork := true
