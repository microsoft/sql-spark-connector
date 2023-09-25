name := "perf_vs_old"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.6"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "com.microsoft.azure" % "azure-sqldb-spark" % "1.0.2")
