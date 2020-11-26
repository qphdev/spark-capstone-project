name := "capstoneProject"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1")//.map(_ % Provided)

parallelExecution in Test := false
