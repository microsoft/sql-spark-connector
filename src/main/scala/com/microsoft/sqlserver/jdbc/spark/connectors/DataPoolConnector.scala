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
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
 * SaveToDataPools Object implements the core logic to save the given dataframe
 * to SQLServer data pool
 * external table. data source and external table name are passed by the user in parameters
 * 'dataPoolDataSource' and 'dbtable'. Only OverWrite and Append mode are currently supported.
 */
object DataPoolConnector extends Connector with Logging with StrategyType {
  def getType(): String = SQLServerBulkJdbcOptions.DataPoolStrategy

    override def writeInParallel(
        df: DataFrame,
        colMetaData: Array[ColumnMetadata],
        options: SQLServerBulkJdbcOptions,
        appId: String ): Unit = {
        if (options.reliabilityLevel == SQLServerBulkJdbcOptions.BEST_EFFORT) {
            BestEffortDataPoolStrategy.write(df, colMetaData, options, appId)
        }
        else {
            throw new SQLException(
                s"""Invalid value for reliabilityLevel """)
        }
      }

  /*
   * createTable Data pool tables are SQL external table. This function
   * creates a data source and the external table.
   * @param conn Connection to the database
   * @param df DataFrame to use for the schema of the table
   * @param options To get the datasource and tablename to create.
   */
  override def createTable(
      conn: Connection,
      df: DataFrame,
      options: SQLServerBulkJdbcOptions): Unit = {
    logDebug("Creating external table")
    if (!mssqlcheckDataSourceExists(conn, df, options)) {
      logInfo("Datasource does not exist")
      mssqlCreateDataSource(conn, df, options)
    }
    mssqlCreateExTable(conn, df, options)
    logDebug("Created external table successfully")
  }

  /*
   * dropTable This function drops an existing external table.
   * This is used when table needs to be dropped as user specified mode as overwrite
   * @param conn Connection to the database
   * @param dbtable Table name to be dropped
   * @param options To get the datasource and tablename to create.
   */
  override def dropTable(conn: Connection, dbtable: String, options: JDBCOptions): Unit = {
    logDebug("dropTable : Entered")
    val stmt = conn.createStatement()
    try {
      val updateStr = s"DROP EXTERNAL TABLE ${dbtable}"
      val result = stmt.executeUpdate(updateStr)
      logDebug("dropped external table  :" + s"$updateStr")
    } finally {
      logDebug("dropTable : Exited")
      stmt.close()
    }
  }
}
