ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.10.14",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "net.liftweb" %% "lift-json" % "3.5.0"

)

lazy val root = (project in file("."))
  .settings(
    name := "taxitrip"
  )
