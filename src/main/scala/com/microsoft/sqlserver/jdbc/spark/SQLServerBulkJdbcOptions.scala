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

import java.sql.Connection

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite

case class SQLServerBulkJdbcOptions(params: CaseInsensitiveMap[String])
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
  val dataPoolDataSource: String = params.getOrElse("dataPoolDataSource", null)

  // In the standard Spark JDBC implementation, the default isolation level is
  // "READ_UNCOMMITTED," but for SQL Server, the default is "READ_COMMITTED"
  override val isolationLevel: Int =
    params.getOrElse("mssqlIsolationLevel", "READ_COMMITTED") match {
      case "READ_UNCOMMITTED" => Connection.TRANSACTION_READ_UNCOMMITTED
      case "READ_COMMITTED" => Connection.TRANSACTION_READ_COMMITTED
      case "REPEATABLE_READ" => Connection.TRANSACTION_REPEATABLE_READ
      case "SERIALIZABLE" => Connection.TRANSACTION_SERIALIZABLE
      case "SNAPSHOT" => Connection.TRANSACTION_READ_COMMITTED + 4094
    }

  val reliabilityLevel: Int = params.getOrElse("reliabilityLevel", "BEST_EFFORT") match {
    case "BEST_EFFORT" => SQLServerBulkJdbcOptions.BEST_EFFORT
    case "NO_DUPLICATES" => SQLServerBulkJdbcOptions.NO_DUPLICATES
  }

  // batchSize is already defined in JDBCOptions superclass
  val checkConstraints: Boolean = params.getOrElse("checkConstraints", "false").toBoolean
  val fireTriggers: Boolean = params.getOrElse("fireTriggers", "false").toBoolean
  val keepIdentity: Boolean = params.getOrElse("keepIdentity", "false").toBoolean
  val keepNulls: Boolean = params.getOrElse("keepNulls", "false").toBoolean
  val tableLock: Boolean = params.getOrElse("tableLock", "false").toBoolean
  val allowEncryptedValueModifications: Boolean =
    params.getOrElse("allowEncryptedValueModifications", "false").toBoolean

  // Not a feature
  // Only used for internally testing data idempotency
  val testDataIdempotency: Boolean = params.getOrElse("testDataIdempotency", "false").toBoolean

  val dataPoolDistPolicy: String = params.getOrElse("dataPoolDistPolicy", "ROUND_ROBIN")
}

object SQLServerBulkJdbcOptions {
  val BEST_EFFORT: Int = 0
  val NO_DUPLICATES: Int = 1
  val InstanceStrategy: String = "instanceStrategy"
  val DataPoolStrategy: String = "dataPoolStrategy"
}
