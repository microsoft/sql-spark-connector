/**
* Copyright 2020 and onwards Microsoft Corporation
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.microsoft.sqlserver.jdbc.spark

import java.sql.ResultSetMetaData

import org.apache.spark.sql.{DataFrame, Row}

/**
 * Interface to define a read/write strategy.
 * Override write to define a write strategy for the connector.
 * Note Read functionality is re-used from default JDBC connector.
 * Read interface can be defined here in the future if required.
 * */
abstract class DataIOStrategy {
  def write(
        df: DataFrame,
        colMetaData: Array[ColumnMetadata],
        options: SQLServerBulkJdbcOptions,
        appId: String): Unit
}
