package com.microsoft.sqlserver.jdbc.spark

import java.net.{InetAddress, UnknownHostException}
import java.nio.file.{Files, Paths}

import org.apache.spark.deploy.history.LogInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createConnectionFactory
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.annotation.tailrec
import scala.io.Source

/** 
* DataPoolUtils
*
*/
object DataPoolUtils extends Logging {
  /**
   * This method returns a list of data nodes endpoints given a data pool name
   * The implementation used DMVs interface to get configured data pool hostnames.
   * @return a list of data nodes endpoints
   */
  def getDataPoolNodeList(options:SQLServerBulkJdbcOptions): List[String] = {
    logInfo(s"Searching DMV for data pools \n")
    import scala.collection.mutable.ListBuffer
    val stmt = createConnectionFactory(options)().createStatement()
    var nodeList = new ListBuffer[String]()
    val query = s"select address from sys.dm_db_data_pool_nodes"
    try {
      val rs = stmt.executeQuery(query)
      while(rs.next()) {
        val dpNode = rs.getString("address")
        nodeList += dpNode
      }
    } catch  {
      case ex: Exception => {
        logInfo(s"Failed with exception ${ex.getMessage()} \n")
      }
    }
    nodeList.toList
  }

  /**
   * Creates the datapool Connection URL by replacing the hostname in the user provided connection
   * string with the data pool hostname. All user provided parameters in the connection string
   * are retained as is.
   *
   */
  def createDataPoolURL(
                         hostname : String,
                         options: SQLServerBulkJdbcOptions) : String = {
    var tokens = options.url.split(";", 2)
    tokens(0) = s"jdbc:sqlserver://${hostname}:1433"
    val url = tokens.mkString(";")
    url
  }

  /**
   * Utility function to check if user has configured data pools in passed options
   *
   */
  def isDataPoolScenario(options:SQLServerBulkJdbcOptions) : Boolean = {
    (options.dataPoolDataSource != null && options.dataPoolDataSource.length >0)
  }
}
