package com.microsoft.sqlserver.jdbc.spark.integration

import com.microsoft.sqlserver.jdbc.spark.SQLServerBulkJdbcOptions
import com.microsoft.sqlserver.jdbc.spark.utils.JdbcUtils
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.UUID

/**
 * This test class provides an integration test that checks whether the connector can write to and read from SQL server.
 *
 * This test class needs the SQL Server instance from the docker-compose setup.
 * Run `docker compose up` in the root directory of the project for this test to succeed.
 */
class SqlServerReadWriteTests extends SparkFunSuite {
  private val url = "jdbc:sqlserver://localhost:1433;database=master;encrypt=true;trustServerCertificate=true;loginTimeout=30;user=sa;password=secure_password_123;"
  private val dbTable = "model.DataTable"

  private lazy val spark = getSparkSession

  test("Spark should be able to write to and read from SQL server using the connector") {
    import spark.implicits._

    // create the database and tables for testing
    ensureTestSetup()

    // setup the input data
    val inputData = Seq(
      (1, "one"),
      (2, "two")
    )
    val dataToWrite = inputData.toDF("id", "text")

    // write
    dataToWrite.write
      .format("com.microsoft.sqlserver.jdbc.spark")
      .options(Map("url" -> url, "dbtable" -> dbTable))
      .option("dbtable", dbTable)
      .mode(SaveMode.Overwrite)
      .save()

    // read
    val outputData = spark.read
      .format("com.microsoft.sqlserver.jdbc.spark")
      .options(Map("url" -> url, "query" -> s"SELECT * FROM $dbTable"))
      .load()
      .orderBy("id")
      .as[(Int, String)]
      .collect()

    // assert that the input and output data is the same
    assertResult(inputData)(outputData)
  }

  private def getSparkSession: SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    spark
  }

  private def ensureTestSetup(): Unit = {
    val dbName = s"testing_${UUID.randomUUID().toString.replace("-", "_")}"

    val jdbcOptions = new SQLServerBulkJdbcOptions(Map("url" -> url, "dbtable" -> dbTable))

    val jdbcConnection = JdbcUtils.createConnection(jdbcOptions)
    val statement = jdbcConnection.createStatement()

    statement.executeUpdate(s"CREATE DATABASE $dbName")
    statement.executeUpdate(s"USE $dbName")
    statement.executeUpdate("CREATE SCHEMA model")
    statement.executeUpdate("CREATE TABLE model.DataTable (id INT NOT NULL, text NVARCHAR NOT NULL)")
    statement.executeUpdate("DELETE FROM model.DataTable")

    statement.closeOnCompletion()
    jdbcConnection.close()
  }
}
