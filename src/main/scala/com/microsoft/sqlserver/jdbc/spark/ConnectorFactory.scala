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