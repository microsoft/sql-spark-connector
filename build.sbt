name := "spark-mssql-connector"

organization := "com.microsoft.sqlserver.jdbc.spark"

version := "clab-1.0.2"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.6"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  // Spark Testing Utilities
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier
      "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion% "test" classifier
      "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier
      "tests",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",

  //SQLServer JDBC jars
  "com.microsoft.sqlserver" % "mssql-jdbc" % "8.4.1.jre8"
)

scalacOptions := Seq("-unchecked", "-deprecation", "evicted")

// Exclude scala-library from this fat jar. The scala library is already there in spark package.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
sources in (Compile, doc) := Seq.empty