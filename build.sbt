name := "apache-spark"

version := "0.1"

scalaVersion := "2.13.6"

idePackagePrefix := Some("org.example")


libraryDependencies ++= {
  Seq(
    "org.scalatest" %% "scalatest" % "3.2.9",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
  )
}