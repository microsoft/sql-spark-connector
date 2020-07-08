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

import java.sql.{Connection, SQLException}

import com.microsoft.sqlserver.jdbc.spark.connectors.StrategyType
import com.microsoft.sqlserver.jdbc.spark.utils.BulkCopyUtils.{getColMetaData, mssqlTruncateTable}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.tableExists

/**
 * Connector class implements the write for supported SaveModes. Each supported
 * SaveMode implements a defined pattern
 * which is supported by writeInParallel, dropTable and createTable interfaces that can
 * be overridden to implement
 * specific write behaviours. All MsSQLSparkConnectors should derive from Connector class.
 *
 * MssSQLSparkConnector supports following connector strategies that use this interface
 * 1. DefaultConnector - All executors insert into a user specified table directly.
 * Write to table is not transactional and may results in duplicates in executor restart scenarios.
 *
 * 2. ReliableConnectorStagingTable - Implements 2 Phase commit for a transactional write
 * to user table.
 * In Phase 1 executors do an idempotent insert to Staging tables and Phase
 * 2 the driver combines all
 * staging tables to transactionally write to user specified table.
 *
 * 3. ReliableConnectorDTC - Will use XA/DTC to implement transactional
 * write with multiple executors.
 * Implementation is TBC as of now.
 *
 */
abstract class Connector extends Logging with StrategyType {
  var paramOptions = CaseInsensitiveMap[String](Map.empty)

  /**
   * write implements a pattern for supported SaveModes.Each supported
   * SaveMode implements a defined pattern
   * for consistency e.g. Overwrite mode table is dropped or truncated based
   * on isTruncateOption.
   * write is supported by writeInParallel, dropTable and createTable interfaces
   * that can be overridden
   * to implement specific write behaviours.
   * @param sqlContext SQLContext passed from spark jdbc datasource framework.
   * @param mode SaveMode as passed from spark jdbc datasource framework
   * @param df Raw DataFrame passed from spark data source framework
   * @param conn Connection as passed down from jdbc framework
   * @param options SQLServerBulkJdbcOptions passed as a parameter map
   */
  final def write(
      sqlContext: SQLContext,
      mode: SaveMode,
      df: DataFrame,
      conn: Connection,
      options: SQLServerBulkJdbcOptions): Unit = {
    logDebug("write : Entered")
    try {
      if (tableExists(conn, options)) processExistingTable(sqlContext, conn, mode, df, options)
      else processNewTable(sqlContext, conn, df, options)
    } finally logDebug("write : Exiting")
  }

  /**
   * writeInParallel distributes the given dataframe to executors to write.
   * Respective connector implementations
   * can override this method to implement specific parallelization.
   * @param df dataframe to write
   * @param colMetaData column meta data of the table.
   * @param options user provided options.
   * @param appId of the spark application.
   */
  def writeInParallel(
      df: DataFrame,
      colMetaData: Array[ColumnMetadata],
      options: SQLServerBulkJdbcOptions,
      appId: String)

  /**
   * createTable interface. Respective connector implementations can override
   * this to implement specific functionality
   * e.g. data pool connectors  create external table as opposed to data base table.
   * @param conn JDBCConnection to use   *
   * @param df dataframe to write
   * @param options user provided options.
   */
  def createTable(conn: Connection, df: DataFrame, options: SQLServerBulkJdbcOptions)

  /**
   * dropTable interface. Respective connector implementations can override this to
   * implement specific functionality
   * e.g. data pool connectors works with external tables that need to have specific drop syntax.
   * @param conn JDBCConnection to use   *
   * @param dbtable dbtable
   * @param options user provided options.
   */
  def dropTable(conn: Connection, dbtable: String, options: JDBCOptions): Unit

  def processNewTable(
      sqlContext: SQLContext,
      conn: Connection,
      df: DataFrame,
      options: SQLServerBulkJdbcOptions): Unit = {
    logDebug(s"Table '${options.dbtable} does not exist'")
    createTable(conn, df, options)
    val colMetaData = getColMetaData(df, conn, sqlContext, options, true)
    writeInParallel(df, colMetaData, options, sqlContext.sparkContext.applicationId)
  }

  def processExistingTable(
      sqlContext: SQLContext,
      conn: Connection,
      mode: SaveMode,
      df: DataFrame,
      options: SQLServerBulkJdbcOptions): Unit = {
    mode match {
      case SaveMode.Overwrite =>
        if (options.isTruncate) {
          logInfo(s"Overwriting with truncate for table '${options.dbtable}'")
          mssqlTruncateTable(conn, options.dbtable)
        } else {
          logInfo(s"Overwriting without truncate for table '${options.dbtable}'")
          dropTable(conn, options.dbtable, options)
          createTable(conn, df, options)
        }

      case SaveMode.Append =>
        logInfo(s"Appending to table '${options.dbtable}'")

      case SaveMode.ErrorIfExists =>
        throw new SQLException(s"""Error with SaveMode 'ErrorIfExists':
                                Table '${options.dbtable}' already exists""")

      case SaveMode.Ignore =>
        logInfo(s"Table '${options.dbtable}' already exists and SaveMode is Ignore")
        return
    }
    val colMetaData = getColMetaData(df, conn, sqlContext, options, true)
    writeInParallel(df, colMetaData, options, sqlContext.sparkContext.applicationId)
  }
  def params(): CaseInsensitiveMap[String] = paramOptions

  def params(params: Map[String, String]): this.type = {
    paramOptions = CaseInsensitiveMap[String](params)
    this
  }
}
