/*
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.sqlserver.jdbc.spark.utils

import com.microsoft.sqlserver.jdbc.spark.SQLServerBulkJdbcOptions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory

/**
 * DataPoolUtils
 *
 */
object DataPoolUtils extends Logging {

  /**
   * This method returns a list of data nodes endpoints given a data pool name
   * The implementation used DMVs interface to get configured data pool hostnames.
   * @return a list of data nodes endpoints
   */
  def getDataPoolNodeList(options: SQLServerBulkJdbcOptions): List[String] = {
    logInfo(s"Searching DMV for data pools \n")
    import scala.collection.mutable.ListBuffer
    val stmt = createConnectionFactory(options)().createStatement()
    var nodeList = new ListBuffer[String]()
    val query = s"select address from sys.dm_db_data_pool_nodes"
    try {
      val rs = stmt.executeQuery(query)
      while (rs.next()) {
        val dpNode = rs.getString("address")
        nodeList += dpNode
      }
    } catch {
      case ex: Exception => logInfo(s"Failed with exception ${ex.getMessage} \n")
    }
    nodeList.toList
  }

  /**
   * Creates the datapool Connection URL by replacing the hostname in the user provided connection
   * string with the data pool hostname. All user provided parameters in the connection string
   * are retained as is.
   *
   */
  def createDataPoolURL(hostname: String, options: SQLServerBulkJdbcOptions): String = {
    val tokens = options.url.split(";", 2)
    tokens(0) = s"jdbc:sqlserver://${hostname}:1433"
    tokens.mkString(";")
  }

  /**
   * Utility function to check if user has configured data pools in passed options
   *
   */
  def isDataPoolScenario(options: SQLServerBulkJdbcOptions): Boolean = {
    (options.dataPoolDataSource != null && options.dataPoolDataSource.length > 0)
  }
}
