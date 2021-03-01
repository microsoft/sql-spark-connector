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

import java.sql.SQLException

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
  def get(options: SQLServerBulkJdbcOptions) : Connector = {
    if (!DataPoolUtils.isDataPoolScenario(options)) {
      return SingleInstanceConnector
    } else {
      return DataPoolConnector
    }
  }
}