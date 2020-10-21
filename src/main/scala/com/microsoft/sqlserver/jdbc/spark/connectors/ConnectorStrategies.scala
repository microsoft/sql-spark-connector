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
