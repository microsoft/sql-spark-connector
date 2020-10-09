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

import org.apache.spark.sql.DataFrame

object NullConnectorStrategy extends DataIOStrategy with ConnectorStrategy with StrategyType {

  def getType(): String = SQLServerBulkJdbcOptions.InstanceStrategy
  def write(
      df: DataFrame,
      dfColMetaData: Array[ColumnMetadata],
      options: SQLServerBulkJdbcOptions,
      appId: String): Unit = {}
  override def strategy(): Int = -1
}
