/**
 * Copyright (C) 2021 Ferlab Ste-Justine contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

scalaVersion in ThisBuild := "2.12.13"

scalacOptions ++= Seq("-deprecation", "-Ypartial-unification")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val sparkVersion = "3.0.2"
val deltaCoreVersion = "0.8.0"
val typesafeVersion = "1.4.1"
val catsVersion = "2.2.0"
val scalatestVersion = "3.2.0"
val glowVersion = "0.6.0"

val providedLibrairies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "io.delta"         %% "delta-core" % deltaCoreVersion,
  "com.typesafe"     %  "config"     % typesafeVersion
)
import ReleaseTransformations._
val releaseSteps = Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  //runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommand("publishSigned"),
  //releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)

lazy val root = (project in file("."))
  .settings(name := "datalake-lib")
  .settings(sonatypeCredentialHost := "s01.oss.sonatype.org")
  .aggregate(`datalake-core`)

lazy val `datalake-core` = (project in file("datalake-core"))
  .settings(libraryDependencies += "org.apache.spark"      %% "spark-core" % sparkVersion % Provided)
  .settings(libraryDependencies += "org.apache.spark"      %% "spark-sql"  % sparkVersion % Provided)
  .settings(libraryDependencies += "io.delta"              %% "delta-core" % deltaCoreVersion % Provided)
  .settings(libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.14.1")
  .settings(libraryDependencies += "org.typelevel"         %% "cats-core"  % catsVersion)
  .settings(libraryDependencies += "org.scalatest"         %% "scalatest"  % scalatestVersion % Test)
  .settings(libraryDependencies += "io.projectglow"        %% "glow-spark3"% glowVersion  exclude ("org.apache.hadoop", "hadoop-client"))
  .settings(parallelExecution in test := false)
  .settings(sonatypeCredentialHost := "s01.oss.sonatype.org")
  .settings(releaseProcess := releaseSteps)
  .settings(releasePublishArtifactsAction := PgpKeys.publishSigned.value)
  .settings(credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials"))
  .settings(fork := true)
