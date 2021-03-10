/**
 * Copyright (C) 2021 Ferlab Ste-Justine contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

scalaVersion in ThisBuild := "2.12.13"

scalacOptions ++= Seq("-deprecation")
scalacOptions += "-Ypartial-unification"

val sparkVersion = "3.0.2"
val deltaCoreVersion = "0.8.0"
val testNgVersion = "6.14.3"
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

lazy val root = (project in file("."))
  .settings(name := "datalake-lib")
  .settings(version := "0.1.0-SNAPSHOT")
  .aggregate(`datalake-core`)

lazy val `datalake-core` = (project in file("datalake-core"))
  .settings(libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion)
  .settings(libraryDependencies += "org.apache.spark" %% "spark-sql"  % sparkVersion)
  .settings(libraryDependencies += "io.delta"         %% "delta-core" % deltaCoreVersion)
  //.settings(libraryDependencies += "com.typesafe"     %  "config"     % typesafeVersion)
  .settings(libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.14.1")
  .settings(libraryDependencies += "org.typelevel"    %% "cats-core"  % catsVersion)
  .settings(libraryDependencies += "org.scalatest"    %% "scalatest"  % scalatestVersion % Test)
  .settings(libraryDependencies += "io.projectglow"   %% "glow-spark3"% glowVersion)
  .settings(version := "0.1.0-SNAPSHOT")
  .settings(coverageMinimum := 90)


credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

