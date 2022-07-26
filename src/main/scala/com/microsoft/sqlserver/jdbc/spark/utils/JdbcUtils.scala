package com.microsoft.sqlserver.jdbc.spark.utils

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialects

import java.sql.Connection

object JdbcUtils {
  /**
   * Creates a JDBC connection using the input JDBC options.
   * @param options The options which are used to create the connection.
   * @return A JDBC connection.
   */
  def createConnection(options: JDBCOptions): Connection = {
    val dialect = JdbcDialects.get(options.url)
    val conn = dialect.createConnectionFactory(options)(-1)
    conn
  }
}
