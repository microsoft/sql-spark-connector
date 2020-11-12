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
