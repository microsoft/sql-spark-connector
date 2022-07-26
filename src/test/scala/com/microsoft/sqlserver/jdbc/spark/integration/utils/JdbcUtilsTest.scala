package com.microsoft.sqlserver.jdbc.spark.integration.utils

import com.microsoft.sqlserver.jdbc.spark.SQLServerBulkJdbcOptions
import com.microsoft.sqlserver.jdbc.spark.utils.JdbcUtils
import org.apache.spark.SparkFunSuite

/**
 * This test class needs the SQL Server instance from the docker-compose setup.
 * Run `docker compose up` in the root directory of the project for this test to succeed.
 */
class JdbcUtilsTest extends SparkFunSuite {
  test("createConnection should create a connection for valid options") {
    val url = "jdbc:sqlserver://localhost:1433;database=master;encrypt=true;trustServerCertificate=true;loginTimeout=30;user=sa;password=secure_password_123;"
    val options = Map(
      "url" -> url,
      "dbtable" -> "schema.Table"
    )
    val jdbcOptions = new SQLServerBulkJdbcOptions(options)

    val connection = JdbcUtils.createConnection(jdbcOptions)

    // the connection factory automatically opens the connection, so if the return value is not null it means that we were able to connect
    assert(connection != null)

    // if this does not throw an exception it means that the connection was open and worked
    connection.close()
  }
}
