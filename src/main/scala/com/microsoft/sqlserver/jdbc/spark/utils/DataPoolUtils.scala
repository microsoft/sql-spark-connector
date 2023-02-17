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

import com.microsoft.sqlserver.jdbc.spark.utils.JdbcUtils.createConnection

import java.net.{InetAddress, UnknownHostException}
import java.nio.file.{Files, Paths}
import org.apache.spark.deploy.history.LogInfo
import org.apache.spark.internal.Logging
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
    val stmt = createConnection(options).createStatement()
    val nodeList = new ListBuffer[String]()
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
