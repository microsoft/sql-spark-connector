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

import com.microsoft.sqlserver.jdbc.spark.Connector

object ConnectorStrategies {
  var cache: Iterable[ConnectorStrategy] = Iterable[ConnectorStrategy]()

  def reliabilityMatches(conn: Connector, connStrategy: ConnectorStrategy): Boolean =
    conn
      .params()
      .get("reliabilityLevel")
      .contains(connStrategy.strategy().toString)

  def strategyTypeMatches(conn: Connector, connStrategy: ConnectorStrategy): Boolean =
    conn.getType() == connStrategy.asInstanceOf[StrategyType].getType()

  def strategyMatches(conn: Connector, connStrategy: ConnectorStrategy): Boolean =
    reliabilityMatches(conn, connStrategy) && strategyTypeMatches(conn, connStrategy)

  /**
   * Allows for matching connector types (data pool, instance, etc) to their
   * respective reliability level.
   */
  def get(connector: Connector): DataIOStrategy =
    cache
      .find(strategyMatches(connector, _))
      .getOrElse(NullConnectorStrategy)
      .asInstanceOf[DataIOStrategy]

  def register(connStrategy: ConnectorStrategy): Unit =
    cache = cache ++ Iterable[ConnectorStrategy](connStrategy)
}
