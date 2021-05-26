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

import java.sql.Connection

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{
  JDBCOptions,
  JdbcOptionsInWrite
}

class SQLServerBulkJdbcOptions(val params: CaseInsensitiveMap[String])
    extends JdbcOptionsInWrite(params) {

  def this(params: Map[String, String]) = this(CaseInsensitiveMap(params))

  // Save original parameters for when a JdbcBulkOptions instance is passed
  // from the Spark driver to an executor, which loses the reference to the
  // params input in memory
  override val parameters = params

  val dbtable = params.getOrElse("dbtable", null)

  val user = params.getOrElse("user", null)
  val password = params.getOrElse("password", null)

  // If no value is provided, then we write to a single SQL Server instance.
  // A non-empty value indicates the name of a data source whose location is
  // the data pool that the user wants to write to. This data source will
  // contain the user's external table.
  val dataPoolDataSource = params.getOrElse("dataPoolDataSource", null)

  // In the standard Spark JDBC implementation, the default isolation level is
  // "READ_UNCOMMITTED," but for SQL Server, the default is "READ_COMMITTED"
  override val isolationLevel =
    params.getOrElse("mssqlIsolationLevel", "READ_COMMITTED") match {
      case "READ_UNCOMMITTED" => Connection.TRANSACTION_READ_UNCOMMITTED
      case "READ_COMMITTED"   => Connection.TRANSACTION_READ_COMMITTED
      case "REPEATABLE_READ"  => Connection.TRANSACTION_REPEATABLE_READ
      case "SERIALIZABLE"     => Connection.TRANSACTION_SERIALIZABLE
      case "SNAPSHOT"         => Connection.TRANSACTION_READ_COMMITTED + 4094
    }

  val reliabilityLevel =
    params.getOrElse("reliabilityLevel", "BEST_EFFORT") match {
      case "BEST_EFFORT"   => SQLServerBulkJdbcOptions.BEST_EFFORT
      case "NO_DUPLICATES" => SQLServerBulkJdbcOptions.NO_DUPLICATES
    }

  // batchSize is already defined in JDBCOptions superclass
  val checkConstraints = params.getOrElse("checkConstraints", "false").toBoolean
  val fireTriggers = params.getOrElse("fireTriggers", "false").toBoolean
  val keepIdentity = params.getOrElse("keepIdentity", "false").toBoolean
  val keepNulls = params.getOrElse("keepNulls", "false").toBoolean
  val tableLock = params.getOrElse("tableLock", "false").toBoolean
  val allowEncryptedValueModifications =
    params.getOrElse("allowEncryptedValueModifications", "false").toBoolean


  val schemaCheckEnabled =
    params.getOrElse("schemaCheckEnabled", "true").toBoolean

  // Not a feature
  // Only used for internally testing data idempotency
  val testDataIdempotency =
    params.getOrElse("testDataIdempotency", "false").toBoolean

  val dataPoolDistPolicy = params.getOrElse("dataPoolDistPolicy", "ROUND_ROBIN")
}

object SQLServerBulkJdbcOptions {
  val BEST_EFFORT = 0
  val NO_DUPLICATES = 1
}
