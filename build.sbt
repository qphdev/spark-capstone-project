name := "capstoneProject"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.2" % Test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test
