package com.microsoft.sqlserver.jdbc.spark

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException}

import com.microsoft.sqlserver.jdbc.spark.BulkCopyUtils.{getColMetaData, getEmptyResultSet, mssqlTruncateTable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.tableExists

/**
 * Connector class implements the write for supported SaveModes. Each supported SaveMode implements a defined pattern
 * which is supported by writeInParallel, dropTable and createTable interfaces that can be overridden to implement
 * specific write behaviours. All MsSQLSparkConnectors should derive from Connector class.
 *
 * MssSQLSparkConnector supports following connector strategies that use this inteface
 * 1. DefaultConnector - All executors insert into a user specified table directly.
 * Write to table is not transactional and may results in duplicates in executor restart scenarios.
 *
 * 2. ReliableConnectorStagingTable - Implements 2 Phase commit for a transactional write to user table.
 * In Phase 1 executors do an idempotent insert to Staging tables and Phase 2 the driver combines all
 * staging tables to transactionally write to user specified table.
 *
 * 3. ReliableConnectorDTC - Will use XA/DTC to implement transactional write with multiple executors.
 * Implementation is TBC as of now.
 *
 */
 abstract class Connector extends Logging {
  /**
   * write implements a pattern for supported SaveModes.Each supported SaveMode implements a defined pattern
   * for consistency e.g. Overwrite mode table is dropped or trucated based on isTruncateOption.
   * write is supported by writeInParallel, dropTable and createTable interfaces that can be overridden
   * to implement specific write behaviours.
   * @param sqlContext SQLContext passed from spark jdbc datasource framework.
   * @param saveMode as passed from spark jdbc datasource framework
   * @param rawdf raw dataframe passed from spark data soruce framework
   * @param parameters User options passed as a parameter map
   */

  final def write(
       sqlContext: SQLContext,
       mode: SaveMode,
       df: DataFrame,
       conn: Connection,
       options: SQLServerBulkJdbcOptions ): Unit = {
    logDebug("write : Entered")
    try {
      if (tableExists(conn, options)) {
        mode match {
          case SaveMode.Overwrite =>
            if(options.isTruncate) {
              logInfo(s"Overwriting with truncate for table '${options.dbtable}'")
              val colMetaData = getColMetaData(df, conn, sqlContext, options, true)
              mssqlTruncateTable(conn, options.dbtable)
              writeInParallel(df, colMetaData, options, sqlContext.sparkContext.applicationId)
            }
            else {
              logInfo(s"Overwriting without truncate for table '${options.dbtable}'")
              dropTable(conn, options.dbtable, options)
              createTable(conn, df, options)
              val colMetaData = getColMetaData(df, conn, sqlContext, options, false)
              writeInParallel(df, colMetaData, options, sqlContext.sparkContext.applicationId)
            }
          case SaveMode.Append =>
            logInfo(s"Appending to table '${options.dbtable}'")
            val colMetaData = getColMetaData(df, conn, sqlContext, options,true)
            writeInParallel(df, colMetaData, options, sqlContext.sparkContext.applicationId)
          case SaveMode.ErrorIfExists =>
            throw new SQLException(
              s"""Error with SaveMode 'ErrorIfExists':
                                Table '${options.dbtable}' already exists""")
          case SaveMode.Ignore =>
            logInfo(s"Table '${options.dbtable}' already exists and SaveMode is Ignore")
        }
      } else {
        logDebug(s"Table '${options.dbtable} does not exist'")
        createTable(conn, df, options)
        val colMetaData = getColMetaData(df, conn, sqlContext, options,false)
        writeInParallel(df, colMetaData, options, sqlContext.sparkContext.applicationId)
      }
    } finally {
      logDebug("write : Exiting")
    }
  }

  /**
   * writeInParallel distributes the given dataframe to executors to write. Respective connector implementations
   * can override this method to implement specific parallelization.
   * @param df dataframe to write
   * @param colMetaData column meta data of the table.
   * @param options user provided options.
   * @param appId of the spark application.
   */
  def writeInParallel(
        df: DataFrame,
        colMetaData: Array[ColumnMetadata],
        options: SQLServerBulkJdbcOptions,
        appId: String): Unit

  /**
   * createTable interface. Respective connector implementations can override this to implement specific functionality
   * e.g. data pool connectors  create external table as opposed to data base table.
   * @param conn JDBCConnection to use   *
   * @param df dataframe to write
   * @param options user provided options.
   */
  def createTable(
        conn: Connection,
        df: DataFrame,
        options: SQLServerBulkJdbcOptions) : Unit

  /**
   * dropTable interface. Respective connector implementations can override this to implement specific functionality
   * e.g. data pool connectors works with external tables that need to have specific drop syntax.
   * @param conn JDBCConnection to use   *
   * @param dbtable dbtable
   * @param options user provided options.
   */
  def dropTable(
        conn: Connection,
        dbtable: String,
        options: JDBCOptions): Unit
}