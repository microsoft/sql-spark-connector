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

package com.microsoft.sqlserver.jdbc.spark.connectors

import java.sql.Connection

import com.microsoft.sqlserver.jdbc.spark.{ColumnMetadata, Connector, SQLServerBulkJdbcOptions}
import com.microsoft.sqlserver.jdbc.spark.utils.BulkCopyUtils._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

/**
 * SingleInstanceConnector implements the MsSQLSparkConnector to write to Single SQL Instance.
 * This connector supports both 'BEST_EFFORT' and 'NO_DUPLICATES' modes. The mode
 * is selected based on
 * 'reliabilityLevel' option. The default is 'BEST_EFFORT'. Based on the mode the  right
 * write strategy is called.
 */
object SingleInstanceConnector extends Connector with Logging with StrategyType {
  def getType(): String = SQLServerBulkJdbcOptions.InstanceStrategy

  override def writeInParallel(
      df: DataFrame,
      colMetaData: Array[ColumnMetadata],
      options: SQLServerBulkJdbcOptions,
      appId: String): Unit = {
    paramOptions = options.params
    ConnectorStrategyFactory.create(_: Connector).write(df, colMetaData, options, appId)
  }

  override def createTable(
      conn: Connection,
      df: DataFrame,
      options: SQLServerBulkJdbcOptions): Unit = {
    mssqlCreateTable(conn, df, options)
  }

  override def dropTable(conn: Connection, dbtable: String, options: JDBCOptions) {
    JdbcUtils.dropTable(conn, dbtable, options)
  }

}
