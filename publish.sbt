ThisBuild / sonatypeCredentialHost := sys.env.getOrElse("SONATYPE_HOST", "s01.oss.sonatype.org")
ThisBuild / credentials += sys.env.get("SONATYPE_USERNAME").map(username => Credentials(sys.env("SONATYPE_REALM"), sys.env("SONATYPE_HOST"), username, sys.env("SONATYPE_PASSWORD"))).getOrElse(Credentials(Path.userHome / ".sbt" / "sonatype_credentials"))
ThisBuild / releasePublishArtifactsAction := PgpKeys.publishSigned.value
ThisBuild / fork := true
ThisBuild / versionScheme := Some("semver-spec")
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
    id = "botekchristophe",
    name = "Christophe Botek",
    email = "cbotek@ferlab.bio",
    url = url("https://github.com/botekchristophe")
  ),
  Developer(
    id = "jecos",
    name = "Jeremy Costanza",
    email = "jcostanza@ferlab.bio",
    url = url("https://github.com/jecos")
  )
)
ThisBuild / description := "Library built on top of Apache Spark to speed-up data lakes development.."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/Ferlab-Ste-Justine/datalake-lib"))

import sbt.url
import xerial.sbt.Sonatype._

ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("Ferlab Ste-Justine", "datalake-lib", "cbotek@ferlab.bio"))
// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true