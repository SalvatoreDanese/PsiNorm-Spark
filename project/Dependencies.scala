import sbt._

object Dependencies {
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.8"
  lazy val sparkMLlib = "org.apache.spark" %% "spark-mllib" % "2.4.8"
}