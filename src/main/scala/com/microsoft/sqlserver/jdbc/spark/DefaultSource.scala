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

package com.microsoft.sqlserver.jdbc.spark

import com.microsoft.sqlserver.jdbc.spark.utils.BulkCopyUtils._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.apache.spark.sql.sources.BaseRelation

/**
 * DefaultSource extends JDBCRelationProvider to provide a implementation for
 * MSSQLSpark connector.  Only write function is overridden.
 * Read functionality not overridden and is re-used from default JDBC connector.
 * Read for datapool external tables is supported from Master instance that's
 * handled via JDBC logic.
 */
class DefaultSource extends JdbcRelationProvider with Logging {

  /**
   * shortName overrides datasource interface to provide an alias to access mssql spark connector.
   */
  override def shortName(): String = "mssql"

  /**
   * createRelation overrides createRelations from JdbcRelationProvider to implement custom write
   * for both SQLServer Master instance and Data pools. The choice is made at run time based on
   * based on the passed parameter map.
   * @param sqlContext SQLContext passed from spark jdbc datasource framework.
   * @param mode as passed from spark jdbc datasource framework
   * @param parameters User options passed as a parameter map
   * @param rawDf raw dataframe passed from spark data source framework
   *
   */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      rawDf: DataFrame): BaseRelation = {
    val options = new SQLServerBulkJdbcOptions(parameters)
    val conn = createConnectionFactory(options)()
    val df = repartitionDataFrame(rawDf, options)

    logDebug("createRelations: Write request. Connection catalogue is" + s"${conn.getCatalog}")
    logDebug(
      s"createRelations: Write request. ApplicationId is ${sqlContext.sparkContext.applicationId}")
    try {
      checkIsolationLevel(conn, options)
      val connector = ConnectorFactory.get(options)
      connector.write(sqlContext, mode, df, conn, options)
    } finally {
      logDebug("createRelations: Closing connection")
      conn.close()
    }
    logDebug("createRelations: Exiting")
    super.createRelation(sqlContext, parameters)
  }
}
