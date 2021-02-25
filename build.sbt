/**
 * Copyright (C) 2021 Ferlab Ste-Justine contributors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 */

organization in ThisBuild := "ca.ferlab"

scalaVersion in ThisBuild := "2.12.11"

scalacOptions ++= Seq("-deprecation")
scalacOptions += "-Ypartial-unification"

val sparkVersion = "3.0.1"
val deltaCoreVersion = "0.8.0"
val testNgVersion = "6.14.3"
val typesafeVersion = "1.2.1"
val catsVersion = "2.0.0"

val providedLibrairies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "io.delta"         %% "delta-core" % deltaCoreVersion,
  "com.typesafe"     %  "config"     % typesafeVersion
)

lazy val root = (project in file("."))
  .settings(name := "datalake-lib")
  .aggregate(`datalake-core`)

lazy val `datalake-core` = (project in file("datalake-core"))
  .settings(libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion)
  .settings(libraryDependencies += "org.apache.spark" %% "spark-sql"  % sparkVersion)
  .settings(libraryDependencies += "io.delta"         %% "delta-core" % deltaCoreVersion)
  .settings(libraryDependencies += "com.typesafe"     %  "config"     % typesafeVersion)
  .settings(libraryDependencies += "org.typelevel"    %% "cats-core"  % catsVersion)
  .settings(libraryDependencies += "org.scalatest"    %% "scalatest"  % "3.2.0" % Test)
  .settings(libraryDependencies += "io.projectglow"   %% "glow-spark3"% "0.6.0")
  .settings(version := "0.0.1")
  .settings(coverageMinimum := 90)
