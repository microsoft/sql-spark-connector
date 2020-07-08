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

import com.microsoft.sqlserver.jdbc.spark.{ColumnMetadata, SQLServerBulkJdbcOptions}
import com.microsoft.sqlserver.jdbc.spark.utils.BulkCopyUtils.savePartition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 * Implements the BEST_EFFORT write strategy for Single Instance. All executors insert
 * into a user specified table directly. Write to table is not transactional and
 * may results in duplicates in executor restart scenarios.
 */
object BestEffortSingleInstanceStrategy
    extends DataIOStrategy
    with Logging
    with ConnectorStrategy
    with StrategyType {
  def getType(): String = SQLServerBulkJdbcOptions.InstanceStrategy

  /**
   * write implements the default write strategy. Each executor writes a
   * partition of data to SQL instance. The executors will retry under failures
   * and this may result in duplicates
   */
  def write(
      df: DataFrame,
      colMetaData: Array[ColumnMetadata],
      options: SQLServerBulkJdbcOptions,
      appId: String): Unit = {
    logInfo("write : best effort write to single instance called")

    val dfColMetadata = colMetaData
    df.rdd.foreachPartition(iterator =>
      savePartition(iterator, options.dbtable, dfColMetadata, options))
  }
  def strategy(): Int = {
    SQLServerBulkJdbcOptions.BEST_EFFORT
  }
}
