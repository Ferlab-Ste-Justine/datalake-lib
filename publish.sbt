//ThisBuild / sonatypeCredentialHost := {
//  val resolvedHost = sys.env.getOrElse("SONATYPE_HOST", "ossrh-staging-api.central.sonatype.com")
//  println(s"[DEBUG] sonatypeCredentialHost = $resolvedHost")
//  resolvedHost
//}
//ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / credentials += sys.env.get("SONATYPE_USERNAME").map { username =>
  val realm = sys.env("SONATYPE_REALM")
  val host = sys.env("SONATYPE_HOST")
  val password = sys.env("SONATYPE_PASSWORD")
  val spacedOutUsername = username.flatMap(c => s"$c ")
  println(s"[DEBUG] Using env credentials: $realm @ $host for user $spacedOutUsername")
  Credentials(realm, host, username, password)
}.getOrElse {
  val credentialsFile = Path.userHome / ".sbt" / "sonatype_credentials"
  println("[DEBUG] Falling back to ~/.sbt/sonatype_credentials")

  if (credentialsFile.exists()) {
    println("[DEBUG] Contents (excluding password):")
    IO.readLines(Path.userHome / ".sbt" / "sonatype_credentials")
      .filterNot(_.toLowerCase.startsWith("password"))
      .foreach(line => println(s"  $line"))
  }
  Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
}
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
//import xerial.sbt.Sonatype._

//ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("Ferlab Ste-Justine", "datalake-lib", "cbotek@ferlab.bio"))
// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
ThisBuild / publishMavenStyle := true