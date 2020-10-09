/*
 * Copyright (c) 2020 Microsoft Corporation
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
