import xerial.sbt.Sonatype._

publishMavenStyle := true

sonatypeProfileName := "bio.ferlab"
sonatypeProjectHosting := Some(GitHubHosting(user="Ferlab-Ste-Justine", repository="datalake-lib", email="cbotek@ferlab.bio"))
developers := List(
  Developer(id = "botekchristophe", name = "C. Botek", email = "cbotek@ferlab.bio", url = url("https://github.com/botekchristophe"))
)
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

publishTo := sonatypePublishToBundle.value