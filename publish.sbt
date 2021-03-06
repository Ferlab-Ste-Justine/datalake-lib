ThisBuild / organization := "bio.ferlab"
ThisBuild / organizationName := "ferlab"
ThisBuild / organizationHomepage := Some(url("https://github.com/Ferlab-Ste-Justine"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Ferlab-Ste-Justine/datalake-lib"),
    "scm:git@github.com:Ferlab-Ste-Justine/datalake-lib.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "botekchristophe",
    name  = "Christophe Botek",
    email = "cbotek@ferlab.bio",
    url   = url("https://github.com/botekchristophe")
  )
)

ThisBuild / description := "Library built on top of Apache Spark to speed-up data lakes development.."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/Ferlab-Ste-Justine/datalake-lib"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }

ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true