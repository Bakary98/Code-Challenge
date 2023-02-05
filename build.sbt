version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "CodeChallengeBelieve"
  )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "io.delta" %% "delta-core" % "0.8.0"



)
