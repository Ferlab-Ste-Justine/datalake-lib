ThisBuild / githubTokenSource := TokenSource.Or(
  TokenSource.Environment("GITHUB_TOKEN"), // Injected during a github workflow for publishing
  TokenSource.GitConfig("github.token") // local token set in ~/.gitconfig
)

ThisBuild / githubOwner := "Ferlab-Ste-Justine"
ThisBuild / githubRepository := "datalake-lib"

ThisBuild / publishMavenStyle := true