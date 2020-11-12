package com.microsoft.sqlserver.jdbc.spark

import java.sql.{Connection, ResultSetMetaData, SQLException}

import com.microsoft.sqlserver.jdbc.spark.BulkCopyUtils.{executeUpdate, savePartition}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Implements the Reliable write strategy for Single Instances that's that's resilient to executor restart.
 * Write is implemented as a 2 phase commit and executor restarts do not result in duplicate inserts.
 */

object ReliableSingleInstanceStrategy extends  DataIOStrategy with Logging {

  /**
   * Implements a reliable write strategy that's resilient to executor restart.
   * A 2 Phase commit is implemented for a transactional write to user table.
   * Phase 1 Driver create global temporary tables for each executors and then invokes the executors
   * do an idempotent insert of the given partition to these temporary staging tables.
   * Phase 2 Driver combines all staging tables to transactionally write to user specified table.
   * Driver does a cleanup of staging tables as a good practice. Staging tables are temporary tables
   * and should be cleanup automatically on job completion. Staging table names are prefixed
   * with appId to allow for identification.
   * @param df dataframe to write
   * @param dfColMetaData for the table
   * @param options user specified options
   * @param appId applicationId that's used as prefix for staging tables.
   */
  def write(
         df: DataFrame,
         dfColMetaData: Array[ColumnMetadata],
         options: SQLServerBulkJdbcOptions,
         appId: String): Unit = {
    logInfo("write : reliable write to single instance called")
    // Initialize - create connection and cleanup existing tables if any
    val conn = createConnectionFactory(options)()
    val stagingTableList = getStagingTableNames(appId, df.rdd.getNumPartitions)
    cleanupStagingTables(conn, stagingTableList, options)
    createStagingTables(conn, stagingTableList,options)
    // Phase1 - Executors write partitions to staging tables.
    logDebug(s"write : Starting Phase 1 - Insert to Staging tables")
    val phase1Success = writeToStagingTables(df, dfColMetaData, options, appId)

    // Phase 2 - Check executor results, Union to create final table or fail is any executor
    // failed despite retries.
    logDebug(s"write : Starting Phase 2 - Union Staging tables")
    phase1Success match {
      case true =>
        logDebug(s"*** write : Initiating unionStagingTables")
        if(stagingTableList.length > 0 ){
          // length check for empty data frame write scenario
          unionStagingTables(conn, stagingTableList, dfColMetaData, options)
        }
        cleanupStagingTables(conn, stagingTableList, options)
      case false =>
        logDebug(s"*** write : Dropping Phase 2 due to Phase 1 failure")
        cleanupStagingTables(conn, stagingTableList, options)
        throw new SQLException(
          s"""Failed dues to non-transient error. No records written """)
    }
    logDebug(s"write : Finished")
  }

  /**
   * writeToStagingTables invokes executor action on each df partitions
   * @param df dataframe to write
   * @param dfColMetadata array of column meta data for the table
   * @param options user specified options
   * @param appId applicationId that's used as prefix for staging tables.
   */
  private def writeToStagingTables(
                df: DataFrame,
                dfColMetadata: Array[ColumnMetadata],
                options: SQLServerBulkJdbcOptions,
                appId: String): Boolean = {
    var allSuccess = true
    try {
      df.rdd.mapPartitionsWithIndex(
        (index, iterator) => {
          val table_name = getStagingTableName(appId,index)
          logDebug(s"writeToStagingTables: Writing partition index $index to Table $table_name")
          val newOptions = new SQLServerBulkJdbcOptions(options.parameters + ("tableLock" -> "true"))
          idempotentInsertToTable(iterator, table_name, dfColMetadata, newOptions)
          logInfo(s"writeToStagingTables: Successfully wrote partition $index to Table $table_name")
          Iterator[Int](1)
        }
      ).collect()
    }
    catch {
      case ex: Exception =>
        allSuccess = false
        logError(s"writeToStagingTables: Executor failed write to table: ${ex.getMessage()}")
    }
    allSuccess
  }

  /**
   * idempotentInsert implements an idempotent insert into the given table.
   * Idempotency is achieved by first dropping the table if it exists,
   * creating a table identical to the main table, followed by saving the given parition
   * to the table using bulk insert.
   * @param iterator rows that represent the partition to be written
   * @param tableName staging table name to while to write the rows.
   * @param options user specified options   *
   */
  private def idempotentInsertToTable(
               iterator: Iterator[Row],
               tableName: String,
               dfColMetaData: Array[ColumnMetadata],
               options: SQLServerBulkJdbcOptions): Unit = {
    logDebug(s"idempotentInsertToTable : Started")
    val conn = createConnectionFactory(options)()
    try {
      BulkCopyUtils.mssqlTruncateTable(conn, tableName)
    } catch {
      case ex: SQLException => {
        logError(s"idempotentInsertToTable : Exception during drop table:" + ex.getMessage())
      }
    }
    savePartition(iterator, tableName, dfColMetaData, options)
    FailureInjection.simulateRandomRestart(options)
  }

  /**
   * unionStagingTables inserts into the main table by doing a union of the stagingTables.
   * Note that this is done by using T-SQL and thus this is a transactional operation.
   * @param stagingTableList list of staging tables to union and insert to main table.
   * @param dfColMetadata array of column meta data for the table
   * @param options user specified options
   */
  private def unionStagingTables(
                conn: Connection,
                stagingTableList: IndexedSeq[String],
                dfColMetadata: Array[ColumnMetadata],
                options: SQLServerBulkJdbcOptions): Unit = {
    logInfo("unionStagingTables: insert to final table")
    val insertStmt = stmtInsertWithUnion(stagingTableList, dfColMetadata, options)
    val conn = createConnectionFactory(options)()
    executeUpdate(conn,insertStmt)
  }

  /**
   * utility function to get all global temp table names as a list.
   * @param appId appId used as prefix of tablename
   * @param nrOfPartitions number of paritions in dataframe used as suffix
   */
  private def getStagingTableNames(
                appId: String,
                nrOfPartitions: Int): IndexedSeq[String] = {
    val stagingTableList = for (index <- 0 until nrOfPartitions) yield {
      getStagingTableName(appId, index)
    }
    stagingTableList
  }

  /**
   * utility function to create a staging table name
   * @param appId appId used as prefix of tablename
   * @param index used as suffix
   */
  private def getStagingTableName(
               appId: String,
               index:Int) : String = {
    // Global table names in SQLServer are prefixed with ##
    s"##" + s"$appId" + s"_$index"
  }

  /**
   * utility function to compose a insert statement with union of staging tables
   * @param stagingTableList staging table list
   * @param dfColMetadata columns
   */
  private def stmtInsertWithUnion(
               stagingTableList: IndexedSeq[String],
               dfColMetadata: Array[ColumnMetadata],
               options: SQLServerBulkJdbcOptions): String = {
    logDebug(s"stmtInsertWithUnion: Staging tables to union are ${stagingTableList.mkString(",")}")
    val unionStr = stagingTableList.map(item => s"SELECT * from $item").mkString(" UNION ALL ")
    val colStr = dfColMetadata.map(item => item.getName).mkString(",")
    options.tableLock match {
      case true => {
       s"INSERT INTO ${options.dbtable} WITH (TABLOCK) $unionStr"
      }
      case false => {
        s"INSERT INTO ${options.dbtable} $unionStr"
      }
    }
  }

  /**
   * utility function to drop staging tables
   * @param stagingTableList staging table list
   * @param options user specified options
   */
  private def cleanupStagingTables(
                conn : Connection,
                stagingTableList: IndexedSeq[String],
                options: SQLServerBulkJdbcOptions): Unit = {
    logDebug(s"cleanupStagingTables: Tables to cleanup are ${stagingTableList.mkString(",")}")
    stagingTableList.map(item => try {
      JdbcUtils.dropTable(conn,item, options)
    }
    catch {
      case ex: SQLException => {logError(s"cleanupStagingTables: Exception while dropping table $item :"
                                  + ex.getMessage())}
    })
  }
  /**
   * createStagingTable creates an empty table that's a exact schema copy of the main table.
   * @param conn connection
   * @param tableName staging table name to while to write the rows.
   * @param options user specified options   *
   */
  private def createStagingTable(
               conn: Connection,
               tableName : String,
               options: SQLServerBulkJdbcOptions) : Unit = {
    logDebug(s"createStagingTable : Creating table $tableName as schema copy of ${options.dbtable}")
    val createTableStr = s"SELECT * INTO $tableName From ${options.dbtable} WHERE 1=0"
    executeUpdate(conn,createTableStr)
  }

  /**
   * utility function to create  staging tables
   * @param stagingTableList staging table list
   * @param options user specified options
   */
  private def createStagingTables(
               conn : Connection,
               stagingTableList: IndexedSeq[String],
               options: SQLServerBulkJdbcOptions): Unit = {
    logDebug(s"createStagingTables: Tables to create are ${stagingTableList.mkString(",")}")
    stagingTableList.map(item => try {
      createStagingTable(conn,item, options)
    }
    catch {
      case ex: SQLException => {
        logError(s"createStagingTables: Exception while creating table $item : " + ex.getMessage())
        throw ex}
    })
  }
}
