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

import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{ByteType, DataType, ShortType, StructType}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{createConnectionFactory, getSchema, schemaString}
import com.microsoft.sqlserver.jdbc.{SQLServerBulkCopy, SQLServerBulkCopyOptions}

import scala.collection.mutable.ListBuffer

/**
* BulkCopyUtils Object implements common utility function used by both datapool and 
*/

object BulkCopyUtils extends Logging {
    /**
    * savePartition 
    * Function to write a partition of the dataframe to the database using the BulkWrite APIs. Creates 
    * a connection, sets connection properties and does a BulkWrite. Called when writing data to 
    * master instance and data pools both. URL in options is used to create the relevant connection.
    *
    * @param itertor - iterator for row of the partition.
    * @param dfColMetadata - array of ColumnMetadata type
    * @param options - SQLServerBulkJdbcOptions with url for the connection
    */

    private[spark] def savePartition(
        iterator: Iterator[Row],
        tableName: String,
        dfColMetadata: Array[ColumnMetadata],
        options: SQLServerBulkJdbcOptions ): Unit = {

        logDebug("savePartition:Entered")
        val conn = createConnectionFactory(options)()
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(options.isolationLevel)
        var committed = false

        try {
            logDebug("savePartition: Calling SQL Bulk Copy to write data")
            val sqlServerBulkCopy = new SQLServerBulkCopy(conn)
            bulkWrite(iterator, tableName, sqlServerBulkCopy, dfColMetadata, options)

            conn.commit()
            committed = true
        } catch {
            case e: SQLException =>
                handleException(e)
                throw e
        } finally {
            if (!committed) {
                conn.rollback()
                conn.close()
            } else {
                // Since this partition has been committed, we don't want any
                // exceptions thrown during close() to cause Spark to re-attempt
                // writing this partition to the database
                try {
                    conn.close()
                } catch {
                    case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
                }
                logDebug("savePartition :Exiting")
            }
        }
    }

    /**
    * bulkWrite 
    * utility function to do set bulk write options (as specified by user) and do a Bulkwrite 
    *
    * @param sqlServerBulkCopy - BulkCopy Object initialized with the connection
    * @param iterator - iterator for row of the partition.
    * @param dfColMetadata - array of ColumnMetadata type
    * @param options - SQLServerBulkJdbcOptions with url for the connection
    */

    def bulkWrite(
        iterator: Iterator[Row],
        tableName:String,
        sqlServerBulkCopy: SQLServerBulkCopy,
        dfColMetadata: Array[ColumnMetadata],
        options: SQLServerBulkJdbcOptions ): Unit = {
        logInfo(s"bulkWrite: Tablock option is ${options.tableLock}")
        val bulkConfig = getBulkCopyOptions(options)
        sqlServerBulkCopy.setBulkCopyOptions(bulkConfig)
        sqlServerBulkCopy.setDestinationTableName(tableName)

        for (i <- 0 to dfColMetadata.length-1) {
            sqlServerBulkCopy.addColumnMapping(dfColMetadata(i).getName(), dfColMetadata(i).getName())
        }

        val bulkRecord = new DataFrameBulkRecord(iterator, dfColMetadata)
        sqlServerBulkCopy.writeToServer(bulkRecord)
    }

    /**
    * handleException
    * utility function to process SQLException
    */
    def handleException(e: SQLException): Unit = {
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
            // If there is no cause already, set 'next exception' as cause.
            // If cause is null, it may be because no cause was set yet
            if (e.getCause == null) {
                try {
                    e.initCause(cause)
                } catch {
                    // Or it may be null because the cause was explicitly
                    // initialized to null, in which case this fails.
                    // There is no other way to detect it.
                    case _: IllegalStateException => e.addSuppressed(cause)
                }
            } else {
                e.addSuppressed(cause)
            }
        }
    }

    /**
    * checkIsolationLevel
    * utility function to check supported isolation levels in sql server
    * Isolation levels supported by SQL Server are READ_UNCOMMITTED, READ_COMMITTED, 
    * REPEATABLE_READ, SERIALIZABLE, and SNAPSHOT
    */  
    private[spark] def checkIsolationLevel(
        conn: Connection, 
        options: SQLServerBulkJdbcOptions): Unit = {

        if (!conn.getMetaData.supportsTransactionIsolationLevel(options.isolationLevel)) {
            conn.close()
            throw new SQLException(s"""Isolation level ${options.isolationLevel} not supported by SQL Server""")
        }
    }

    /**
    * repartitionDataFrame
    * utility function to repartition DataFrame to user specified value and throw an exeption 
    * if numPartition set by user is not correct.
    */ 
    private[spark] def repartitionDataFrame(
        df: DataFrame, 
        options: SQLServerBulkJdbcOptions): DataFrame = {            
        options.numPartitions match {
            case Some(n) if n <= 0 => throw new IllegalArgumentException(
                s"""Invalid value '$n' for parameter 'numPartitions'
                in table writing via JDBC. The minimum value is 1.""")
            case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
            case _ => df
        }
    }
    
    /**
    * getEmptyResultSet
    * utility function to get a empty result. The result set to retrieve the meta data. 
    * TODO : Return  ResultSetMetaData and rename the function to getTableMetaData
    */
    private[spark] def getEmptyResultSet(
        conn: Connection, 
        table: String): ResultSet = {
        val queryStr = s"SELECT * FROM ${table} WHERE 1=0;"
        conn.createStatement.executeQuery(queryStr)
    }

    /**
    * getComputedCols
    * utility function to get computed columns.
     * Use computed column names to exclude computed column when matching schema.
    */
    private[spark] def getComputedCols(
        conn: Connection, 
        table: String): List[String] = {
        val queryStr = s"SELECT name FROM sys.computed_columns WHERE object_id = OBJECT_ID('${table}');"
        val computedColRs = conn.createStatement.executeQuery(queryStr)
        val computedCols = ListBuffer[String]()
        while (computedColRs.next()) {
            val colName = computedColRs.getString("name")
            computedCols.append(colName)
        }
        computedCols.toList
    }

    /**
     * dfComputedColCount
     * utility function to get number of computed columns in dataframe.
     * Use number of computed columns in dataframe to get number of non computed column in df,
     * and compare with the number of non computed column in sql table
     */
    private[spark] def dfComputedColCount(
        dfColNames: List[String],
        computedCols: List[String],
        dfColCaseMap: Map[String, String],
        isCaseSensitive: Boolean): Int ={
        var dfComputedColCt = 0
        for (j <- 0 to computedCols.length-1){
            if (isCaseSensitive && dfColNames.contains(computedCols(j)) ||
              !isCaseSensitive && dfColCaseMap.contains(computedCols(j).toLowerCase())
                && dfColCaseMap(computedCols(j).toLowerCase()) == computedCols(j)) {
                dfComputedColCt += 1
            }
        }
        dfComputedColCt
    }


    /**
     * getColMetadataMap
     * Utility function convert result set meta data to array.
     */
    private[spark] def defaultColMetadataMap(
            metadata: ResultSetMetaData): Array[ColumnMetadata] = {
        val result = new Array[ColumnMetadata](metadata.getColumnCount)
        for (idx <- 0 to metadata.getColumnCount-1) {
            result(idx) = new ColumnMetadata(
                metadata.getColumnName(idx+1),
                metadata.getColumnType(idx+1),
                metadata.getPrecision(idx+1),
                metadata.getScale(idx+1),
                idx)
        }
        result
    }

    /**
     * getColMetaData returns the columnMetaData that's used when writing the data frame to SQL.
     * Additionally it also supports a schema check between dataframe and sql table. The schema
     * check validates the following
     * 1. Number of columns
     * 2. Type of columns (sql to spark data type mapping per JDBCUtils)
     * 3. Names of columns
     * @param df: DataFrame,
     * @param conn: Connection,
     * @param sqlContext: SQLContext,
     * @param options: SQLServerBulkJdbcOptions,
     * @param checkSchema: Boolean
     */
    private[spark] def getColMetaData(
            df: DataFrame,
            conn: Connection,
            sqlContext: SQLContext,
            options: SQLServerBulkJdbcOptions,
            checkSchema: Boolean) :
        Array[ColumnMetadata] = {
        val isCaseSensitive = sqlContext.getConf("spark.sql.caseSensitive").toBoolean
        val rs = getEmptyResultSet(conn, options.dbtable)
        val colMetaData = {
            if(checkSchema) {
                checkExTableType(conn, options)
                matchSchemas(conn, options.dbtable, df, rs, options.url, isCaseSensitive, options.schemaCheckEnabled)
            } else {
                defaultColMetadataMap(rs.getMetaData())
            }
        }
        colMetaData
    }

    /**
    * matchSchemas validates the data frame schema against result set of a
    * SQL table that user is trying to write to. The SQL table is considered the source of truth
    * and data frame columns matches against that. The data frame columns can be in a different
    * order than the SQL table.
    * Note that function does case-insensitive or case-sensitive check based on passed preference.
    * isCaseSensitive as True  : data frame column name is expected have exactly the same case as sql column
    * isCaseSensitive is False : data frame column name can have a different case than the sql column case
    * note that both spark and sql should be configured with the same case settings
    * i.e both CaseSensitive or both not CaseSensitive. In scenario where spark case settings is
    * different this check may pass, but there will be an error at write.
    * @param conn: Connection,
    * @param dbtable: String,
    * @param df: DataFrame,
    * @param rs: ResultSet,
    * @param url: String,
    * @param isCaseSensitive: Boolean
    * @param strictSchemaCheck: Boolean
    */
    private[spark] def matchSchemas(
            conn: Connection,
            dbtable: String,
            df: DataFrame,
            rs: ResultSet,
            url: String,
            isCaseSensitive: Boolean,
            strictSchemaCheck: Boolean): Array[ColumnMetadata]= {
        val dfColCaseMap = (df.schema.fieldNames.map(item => item.toLowerCase)
          zip df.schema.fieldNames.toList).toMap
        val dfCols = df.schema

        val tableCols = getSchema(rs, JdbcDialects.get(url))
        val computedCols = getComputedCols(conn, dbtable)

        val prefix = "Spark Dataframe and SQL Server table have differing"

        if (computedCols.length == 0) {
            assertIfCheckEnabled(dfCols.length == tableCols.length, strictSchemaCheck,
                s"${prefix} numbers of columns")
        } else if (strictSchemaCheck) {
            val dfColNames =  df.schema.fieldNames.toList
            val dfComputedColCt = dfComputedColCount(dfColNames, computedCols, dfColCaseMap, isCaseSensitive)
            // if df has computed column(s), check column length using non computed column in df and table.
            // non computed column number in df: dfCols.length - dfComputedColCt
            // non computed column number in table: tableCols.length - computedCols.length
            assertIfCheckEnabled(dfCols.length-dfComputedColCt == tableCols.length-computedCols.length, strictSchemaCheck,
                s"${prefix} numbers of columns")
        }


        val result = new Array[ColumnMetadata](tableCols.length - computedCols.length)
        var nonAutoColIndex = 0

        for (i <- 0 to tableCols.length-1) {
            val tableColName = tableCols(i).name
            var dfFieldIndex = -1
            // set dfFieldIndex = -1 for all computed columns to skip ColumnMetadata
            if (computedCols.contains(tableColName)) {
                logDebug(s"skipping computed col index $i col name $tableColName dfFieldIndex $dfFieldIndex")
            }else{
                var dfColName:String = ""
                if (isCaseSensitive) {
                    dfFieldIndex = dfCols.fieldIndex(tableColName)
                    dfColName = dfCols(dfFieldIndex).name
                    assertIfCheckEnabled(
                        tableColName == dfColName, strictSchemaCheck,
                        s"""${prefix} column names '${tableColName}' and
                        '${dfColName}' at column index ${i} (case sensitive)""")
                } else {
                    dfFieldIndex = dfCols.fieldIndex(dfColCaseMap(tableColName.toLowerCase()))
                    dfColName = dfCols(dfFieldIndex).name
                    assertIfCheckEnabled(
                        tableColName.toLowerCase() == dfColName.toLowerCase(),
                        strictSchemaCheck,
                        s"""${prefix} column names '${tableColName}' and
                        '${dfColName}' at column index ${i} (case insensitive)""")
                }

                logDebug(s"matching Df column index $dfFieldIndex datatype ${dfCols(dfFieldIndex).dataType} " +
                    s"to table col index $i datatype ${tableCols(i).dataType}")
                if(dfCols(dfFieldIndex).dataType == ByteType && tableCols(i).dataType == ShortType) {
                    // TinyInt translates to spark ShortType. Refer https://github.com/apache/spark/pull/27172
                    // Here we handle a case of writing a ByteType to SQL when Spark Core says that its a ShortType.
                    // We can write a ByteType to ShortType and thus we pass that type checking.
                    logDebug(s"Passing valid translation of ByteType to ShortType")
                }
                else {
                    assertIfCheckEnabled(
                        dfCols(dfFieldIndex).dataType == tableCols(i).dataType,
                        strictSchemaCheck,
                        s"${prefix} column data types at column index ${i}." +
                        s" DF col ${dfColName} dataType ${dfCols(dfFieldIndex).dataType} " +
                        s" Table col ${tableColName} dataType ${tableCols(i).dataType} ")
                }
                assertIfCheckEnabled(
                    dfCols(dfFieldIndex).nullable == tableCols(i).nullable,
                    strictSchemaCheck,
                    s"${prefix} column nullable configurations at column index ${i}" +
                        s" DF col ${dfColName} nullable config is ${dfCols(dfFieldIndex).nullable} " +
                        s" Table col ${tableColName} nullable config is ${tableCols(i).nullable}")

                // Schema check passed for element, Create ColMetaData only for non auto generated column
                result(nonAutoColIndex) = new ColumnMetadata(
                    rs.getMetaData().getColumnName(i+1),
                    rs.getMetaData().getColumnType(i+1),
                    rs.getMetaData().getPrecision(i+1),
                    rs.getMetaData().getScale(i+1),
                    dfFieldIndex
                )
                nonAutoColIndex += 1
            }
        }
        result
    }

    /**
     * utility to extract 3 part name from tablename in options.
     * A fully qualified table name can be a
     * 1 part (mytable) ,
     * 2 part(myschema.mytable) or
     * 3 part table name (mydbname.myschema.mytable)
     */
    def get3PartName(options: SQLServerBulkJdbcOptions) : (String,String,String) = {
            val defaultSchema = "dbo"
            val tokens = options.dbtable.split('.')
            tokens.size match {
                case 3 =>(tokens(0), tokens(1), tokens(2))
                case 2 => ("", tokens(0), tokens(1))
                case 1 => ("", defaultSchema, tokens(0))
            }
    }

    /**
     * utility function to check that table type is inline with the user specified options.
     * The check raises an exception if the user options are trying to write a REPLICATED
     * table to a ROUND ROBIN table or vice versa.
     */
    private def checkExTableType(
                    conn: Connection,
                    options: SQLServerBulkJdbcOptions) : Unit = {
        if(DataPoolUtils.isDataPoolScenario(options)) {
            val (_ , schemaName, tableName) = get3PartName(options)
            val tableType = getExternalTableType(conn, schemaName, tableName)
            options.dataPoolDistPolicy match {
                case "REPLICATED" => {
                    assertCondition(tableType == DataPoolTableType.REPLICATED_TABLES,
                        "External table is not of the type REPLICATED")
                }
                case "ROUND_ROBIN" => {
                    assertCondition(tableType == DataPoolTableType.ROUND_ROBIN_TABLES,
                        "External table is not of the type ROUND_ROBIN")
                }

                case _ => {
                    throw new SQLException(
                        s""" Invalid value in dataPoolDistPolicy ${options.dataPoolDistPolicy}  .
                           | Internal feature usage error:""".stripMargin)
                }
            }
        }
    }

    /**
     * utility to get external table type from SQL
     */
    object DataPoolTableType {
        val REPLICATED_TABLES = 1
        val ROUND_ROBIN_TABLES = 2
    }

    def getExternalTableType(conn:Connection, schemaName:String, tableName:String) : Int = {
        val stmt = conn.createStatement()
        val queryStr = s"select distribution_type FROM sys.external_tables where " +
                            s"schema_name(schema_id)='$schemaName' and name='$tableName'"
        val rs = stmt.executeQuery(queryStr)
        rs.next()
        val tableType = rs.getInt("distribution_type")
        tableType
    }

    /**
     * utility to do an executeUpdate on provided SQL string
     * @param conn - connection to database
     * @param updateString - SQL string to run
     */
    private[spark] def executeUpdate(
        conn: Connection,
        updateString: String): Unit = {
        logDebug(s"execute update ${updateString}")
        val stmt = conn.createStatement()
        try{
            stmt.executeUpdate(updateString)
            logDebug("execute update success")
        } catch {
            case ex:Throwable => {
                logError(s"execute update failed with error ${ex.getMessage()}")
                throw ex
            }
        } finally {
            stmt.close()
        }
    }

    /**
    * utility function to truncate table
    * @param conn - connection to database
    * @param dbtable table name to truncate
    */ 
    private[spark] def mssqlTruncateTable(
        conn: Connection, 
        dbtable: String): Unit = {
        logDebug(s"Truncating table ${dbtable}")
        val truncateTableStr = s"TRUNCATE TABLE ${dbtable}"
        executeUpdate(conn,truncateTableStr)
        logDebug("Truncating table succeeded")
    }

    /**
    * utility function to create a table from dataframe
    * @param conn - connection to database
    * @param df - dataframe to contruct the schema
    * @param options - options to create the table
    */
    private[spark] def mssqlCreateTable(
                        conn: Connection,
                        df: DataFrame,
                        options: SQLServerBulkJdbcOptions): Unit = {
        logDebug("Creating table")
        val strSchema = schemaString(df.schema, true, options.url, options.createTableColumnTypes)
        val createTableStr = s"CREATE TABLE ${options.dbtable} (${strSchema}) ${options.createTableOptions}"
        executeUpdate(conn,createTableStr)
        logDebug("Creating table succeeded")
    }

    /**
     * utility function to create an external table from dataframe
     * DataSource should be precreated to use this function.
     * @param conn - connection to database
     * @param df - dataframe to contruct the schema
     * @param options - options to create the table
     */
    private[spark] def mssqlCreateExTable(
        conn: Connection,
        df: DataFrame,
        options: SQLServerBulkJdbcOptions): Unit = {
        logDebug(s"Creating external table ${options.dbtable}")
        val strSchema = schemaString(df.schema, true, "jdbc:sqlserver")
        val createExTableStr =  s"CREATE EXTERNAL TABLE ${options.dbtable} (${strSchema}) " +
          s"WITH (DATA_SOURCE=${options.dataPoolDataSource}, DISTRIBUTION=${options.dataPoolDistPolicy});"
        executeUpdate(conn,createExTableStr)
        logDebug("Creating external table succeeded")
    }

    /**
     * utility function to create a datasource at sqldatapool://controller-svc/default.
     * Assumes that datasource with that name does not exist
     * @param conn - connection to database
     * @param df - dataframe to contruct the schema
     * @param options - options to create the table
     */
    private[spark] def mssqlCreateDataSource(
        conn: Connection,
        df: DataFrame,
        options: SQLServerBulkJdbcOptions): Unit = {
        logDebug(s"Creating datasource ${options.dataPoolDataSource}")
        val externalDataPool = "sqldatapool://controller-svc/default"
        val createDSStr = s"CREATE EXTERNAL DATA SOURCE ${options.dataPoolDataSource} " +
            s"WITH (LOCATION = '${externalDataPool}');"
        executeUpdate(conn,createDSStr)
        logDebug("Creating datasource succeeded")
    }

    /**
     * checks if datasource exists
     * @param conn - connection to database
     * @param df - dataframe to contruct the schema
     * @param options - options to create the table
     */
    private[spark] def mssqlcheckDataSourceExists(
            conn: Connection,
            df: DataFrame,
            options: SQLServerBulkJdbcOptions): Boolean = {
        logDebug("Check if datasource exists")
        var queryStr = s"SELECT 1 FROM sys.external_data_sources WHERE name = '${options.dataPoolDataSource}'"
        logDebug(s"queryString is ${queryStr}")
        val stmt = conn.createStatement()
        try {
            val result = stmt.executeQuery(queryStr)
            val isDataSrcPresent = result.next()
            isDataSrcPresent
        } catch {
            case ex:Throwable => {
                logError(s"Check data source failed with error ${ex.getMessage()}")
                throw ex
            }
        } finally {
            stmt.close()
        }
    }

    /**
    * utility function retrieve bulk copy options from user options
    * @param options - full set of user options
    */ 
    private def getBulkCopyOptions(
        options: SQLServerBulkJdbcOptions): SQLServerBulkCopyOptions = {
        val bulkCopyOptions = new SQLServerBulkCopyOptions

        bulkCopyOptions.setBatchSize(options.batchSize)
        bulkCopyOptions.setBulkCopyTimeout(options.queryTimeout)
        bulkCopyOptions.setCheckConstraints(options.checkConstraints)
        bulkCopyOptions.setFireTriggers(options.fireTriggers)
        bulkCopyOptions.setKeepIdentity(options.keepIdentity)
        bulkCopyOptions.setKeepNulls(options.keepNulls)
        bulkCopyOptions.setTableLock(options.tableLock)
        bulkCopyOptions.setAllowEncryptedValueModifications(options.allowEncryptedValueModifications)

        bulkCopyOptions
    }

    /**
    * utility function to dbname from user specified URL
    * @param url - url per syntax <jdbc>:<sqlserver>:<hostname>:<port>;database=<dbname>;...
    */     
    private[spark] def getDBNameFromURL(
        url:String) : String = {
        val token_sep = ";"
        val dbname_index = 1 

        val dbstring = url.split(token_sep)(dbname_index)     
        val dbname = dbstring.split("=")(1) 
        return dbname
    }

    /**
    * utility to assert a condition and throw an exception if assert fails
    * @param cond - condition
    * @param msg - message to pass in the SQLException
    */ 

    private def assertCondition(
        cond: Boolean, msg: String): Unit = {
        try {
            assert(cond)
        } catch {
            case e: AssertionError => {
                throw new SQLException(msg)
            }
        }
    }

    /**
     * utility to assert if checkEnabled is true, else info log the mesage
     * @param cond - condition
     * @param msg - message to pass in the SQLException
     */

    private def assertIfCheckEnabled(
            cond: Boolean, checkEnabled : Boolean,  msg: String): Unit = {
        if(checkEnabled) {
            assertCondition(cond, msg)
        }
        else{
           logInfo(msg)
        }

    }
}
