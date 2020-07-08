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

import com.microsoft.sqlserver.jdbc.spark.connectors.{DataPoolConnector, SingleInstanceConnector}
import com.microsoft.sqlserver.jdbc.spark.utils.DataPoolUtils

/**
 * Connector factory returns the appropriate connector implementation
 * based on user preferences. For now we have 2 connectors
 * 1. SingleInstanceConnector that writes to a given SQL instance
 * 2. DataPoolConnector that write to data pools in SQL Server Big Data Clusters.
 */
object ConnectorFactory {

  /**
   * get returns the appropriate connector based on user option
   * dataPoolDataSource which indicates write to datapool
   * @param options user specified options
   */
  def get(options: SQLServerBulkJdbcOptions): Connector = {
    if (!DataPoolUtils.isDataPoolScenario(options)) {
      SingleInstanceConnector
    } else {
      DataPoolConnector
    }
  }
}
