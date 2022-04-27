ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "untitled"
  )

//SPARK
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.0"

//NEEDED FOR BATCH PROCESSING VIA SPARK;DOES NOT NEED TO BE IMPORTED INTO FILE TO WORK
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0"

//KAFKA
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.1.0"
