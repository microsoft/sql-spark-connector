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

import com.microsoft.sqlserver.jdbc.spark.{Connector, SQLServerBulkJdbcOptions}
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext

class ConnectorStrategiesTest extends SparkFunSuite with Matchers with SharedSQLContext {
  test("SingleInstanceConnector is set to an instance connector type") {
    SingleInstanceConnector
      .getType()
      .equals(SQLServerBulkJdbcOptions.InstanceStrategy)
  }

  test("DataPoolConnector is set to an instance connector type") {
    DataPoolConnector
      .getType()
      .equals(SQLServerBulkJdbcOptions.DataPoolStrategy)
  }

  test(
    "ReliableSingleInstanceStrategy is selected when " +
      "NO_DUPLICATES reliability level is specified with instance type") {
    val connector = SingleInstanceConnector.params(
      Map[String, String]("reliabilityLevel" ->
        SQLServerBulkJdbcOptions.NO_DUPLICATES.toString))

    val strategy: DataIOStrategy =
      ConnectorStrategyFactory.create(connector: Connector)

    strategy shouldBe ReliableSingleInstanceStrategy
  }

  test(
    "BestEffortSingleInstanceStrategy is selected when " +
      "BEST_EFFORT reliability level is specified with instance type") {
    val connector = SingleInstanceConnector.params(
      Map[String, String]("reliabilityLevel" ->
        SQLServerBulkJdbcOptions.BEST_EFFORT.toString))

    val strategy: DataIOStrategy =
      ConnectorStrategyFactory.create(connector: Connector)

    strategy shouldBe BestEffortSingleInstanceStrategy
  }

  test(
    "BestEffortDataPoolStrategy is selected when " +
      "BEST_EFFORT reliability level is specified with instance type") {
    val connector = DataPoolConnector.params(
      Map[String, String]("reliabilityLevel" ->
        SQLServerBulkJdbcOptions.BEST_EFFORT.toString))

    val strategy: DataIOStrategy =
      ConnectorStrategyFactory.create(connector: Connector)

    strategy shouldBe BestEffortDataPoolStrategy
  }
}
