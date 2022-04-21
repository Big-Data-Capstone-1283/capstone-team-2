ThisBuild / version := "0.1.0-SNAPSHOT"


libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.0"
ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "untitled"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1" % Test

