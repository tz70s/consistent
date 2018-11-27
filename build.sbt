val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.12.7"
)

val scalaTestVersion = "3.0.5"
val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test

lazy val consistent = (project in file("."))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(scalaTest)
  )