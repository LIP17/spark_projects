name := "LEARN-SPARK"

version := "1.0"

scalaVersion := "2.11.8"

lazy val root = Project("LEARN-SPARK", file("."))
  .settings(
    name := "LEARN-SPARK",
    organization := "LIP",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.0"
    )
  )
        