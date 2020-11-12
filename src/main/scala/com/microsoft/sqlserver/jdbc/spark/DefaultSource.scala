package com.microsoft.sqlserver.jdbc.spark

import java.sql.{Connection, ResultSet, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.sources.BaseRelation

import com.microsoft.sqlserver.jdbc.spark.BulkCopyUtils._

/**
 * DefaultSource extends JDBCRelationProvider to provide a implmentation for MSSQLSpark connector.
 * Only write function is overridden. 
 * Read functionality not overridden and is re-used from defualt JDBC connector. 
 * Read for datapool external tables is supported from Master instance that's handled via JDBC logic.
 */
class DefaultSource extends JdbcRelationProvider with Logging {

    /**
     * shortName overides datasource interface to provide an alias to access mssql spark connector. 
     */
    
    override def shortName(): String = "mssql"

    /**
     * createRelation overrides createRelations from JdbcRelationProvider to implement custom write 
     * for both SQLServer Master instance and Data pools. The choice is made at run time based on 
     * based on the passed parameter map.
     * @param sqlContext SQLContext passed from spark jdbc datasource framework.
     * @param mode as passed from spark jdbc datasource framework
     * @param parameters User options passed as a parameter map
     * @param rawDf raw dataframe passed from spark data soruce framework
     * 
     */
     override def createRelation(
                    sqlContext: SQLContext,
                    mode: SaveMode,
                    parameters: Map[String, String],
                    rawDf: DataFrame): BaseRelation = {
        val options = new SQLServerBulkJdbcOptions(parameters)
        val conn = createConnectionFactory(options)()
        val df = repartitionDataFrame(rawDf, options)

        logDebug("createRelations: Write request. Connection catalogue is" + s"${conn.getCatalog()}")
        logDebug(s"createRelations: Write request. ApplicationId is ${sqlContext.sparkContext.applicationId}")
        try {
            checkIsolationLevel(conn, options)
            val connector = ConnectorFactory.get(options)
            connector.write(sqlContext, mode, df, conn, options)
        } finally {
            logDebug("createRelations: Closing connection")
            conn.close()
        }
        logDebug("createRelations: Exiting")
        super.createRelation(sqlContext, parameters)
    }
}
