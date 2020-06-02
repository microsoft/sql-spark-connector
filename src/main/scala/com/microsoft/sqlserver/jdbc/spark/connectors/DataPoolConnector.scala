package com.microsoft.sqlserver.jdbc.spark

import java.sql.{Connection, ResultSetMetaData, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame}
import com.microsoft.sqlserver.jdbc.spark.BulkCopyUtils._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/**
* SaveToDataPools Object implements the core logic to save the given dataframe to SQLServer data pool 
* external table. data source and external table name are passed by the user in parameters 
* 'dataPoolDataSource' and 'dbtable'. Only OverWrite and Append mode are currently supported.
*/

object DataPoolConnector extends Connector with Logging {
    override def writeInParallel(
        df: DataFrame,
        colMetaData: Array[ColumnMetadata],
        options: SQLServerBulkJdbcOptions,
        appId: String ): Unit = {
        if (options.reliabilityLevel == SQLServerBulkJdbcOptions.BEST_EFFORT) {
            BestEffortDataPoolStrategy.write(df, colMetaData, options, appId)
        }
        else {
            throw new SQLException(
                s"""Invalid value for reliabilityLevel """)
        }
    }

    /*
     * createTable Data pool tables are SQL external table. This function
     * creates a data source and the external table.
     * @param conn Connection to the database 
     * @param df DataFrame to use for the schema of the table
     * @param options To get the datasource and tablename to create.
     */
    override def createTable(conn: Connection, df: DataFrame, options: SQLServerBulkJdbcOptions): Unit = {
        logDebug("Creating external table")
        if(!mssqlcheckDataSourceExists(conn, df, options)) {
            logInfo("Datasource does not exist")
            mssqlCreateDataSource(conn, df, options)
        }
        mssqlCreateExTable(conn, df, options)
        logDebug("Created external table successfully")
    }

    /*
     * dropTable This function drops an existing external table.
     * This is used when table needs to be dropped as user specified mode as overwrite
     * @param conn Connection to the database 
     * @param dbtable Table name to be dropped
     * @param options To get the datasource and tablename to create.
     */
    override def dropTable(conn: Connection, dbtable: String, options: JDBCOptions): Unit = {

        logDebug("dropTable : Entered")
        val stmt = conn.createStatement()
        try {
            val updateStr = s"DROP EXTERNAL TABLE ${dbtable}"
            val result = stmt.executeUpdate(updateStr)
            logDebug("dropped external table  :" +s"$updateStr")
        } finally {
            logDebug("dropTable : Exited")
            stmt.close()
        }
    }    
}
