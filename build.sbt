name := "apache-spark"

version := "0.1"

scalaVersion := "2.12.8"

idePackagePrefix := Some("org.example")

val SPARK_VERSION = "3.1.2"

libraryDependencies ++= {
  Seq(
    "org.scalatest" %% "scalatest" % "3.2.9",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    "org.apache.spark" %% "spark-core" % SPARK_VERSION,
    "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
    "mysql" % "mysql-connector-java" % "8.0.23"
  )
}