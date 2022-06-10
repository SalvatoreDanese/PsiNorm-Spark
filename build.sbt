import Dependencies._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "TestSpark1",
    libraryDependencies += sparkCore,
    libraryDependencies += sparkMLlib
  )
