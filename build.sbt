name := "spark-mssql-connector"

organization := "com.microsoft.sqlserver.jdbc.spark"

version := "1.0.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",

  //SQLServer JDBC jars
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8",

)
scalacOptions := Seq("-unchecked", "-deprecation", "evicted")

// Exclude scala-library from this fat jar. The scala library is already there in spark package.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
