/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
