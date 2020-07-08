name := "spark-mssql-connector"

organization := "com.microsoft.sqlserver.jdbc.spark"

version := "1.0.0"

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
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8"
)

scalacOptions := Seq("-unchecked", "-deprecation", "evicted")

// Exclude scala-library from this fat jar. The scala library is already there in spark package.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)


// Checkstyle build dependencies
(checkstyle in Compile) := (checkstyle in Compile).triggeredBy(compile in Compile).value
checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error)

// Code Coverage
coverageEnabled := true
coverageMinimum := 80
coverageFailOnMinimum := true

// scalastyle build dependency
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(Test).toTask("").value
(test in Test) := ((test in Test) dependsOn testScalastyle).value

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value
scalastyleFailOnError := true

// sbt-header
organizationName := "Microsoft Corporation"
startYear := Some(2018)
excludeFilter.in(headerSources) := HiddenFileFilter || "*Excluded.scala"
excludeFilter.in(headerResources) := HiddenFileFilter || "*.xml"
headerLicense := Some(HeaderLicense.MIT("2018", "Microsoft Corporation"))
lazy val myProject = project
    .in(file("."))
    .enablePlugins(AutomateHeaderPlugin)