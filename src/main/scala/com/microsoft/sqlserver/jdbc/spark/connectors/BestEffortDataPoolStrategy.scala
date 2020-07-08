/*
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.sqlserver.jdbc.spark.connectors

import java.sql.SQLException

import com.microsoft.sqlserver.jdbc.spark.{ColumnMetadata, SQLServerBulkJdbcOptions}
import com.microsoft.sqlserver.jdbc.spark.utils.BulkCopyUtils.{getDBNameFromURL, savePartition}
import com.microsoft.sqlserver.jdbc.spark.utils.DataPoolUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Implements the BEST_EFFORT write strategy for Data Pools. All executors insert
 * into a user specified table directly. Write to table is not transactional and
 * may results in duplicates in executor restart scenarios.
 */
object BestEffortDataPoolStrategy
    extends DataIOStrategy
    with Logging
    with ConnectorStrategy
    with StrategyType {
  def getType(): String = SQLServerBulkJdbcOptions.DataPoolStrategy

  /**
   * write finds the datapool host names, maps the hostnames to respective executors,
   * creates connection urls and delegates control to the executors to start the writing process.
   * The Design of interacting with the data pools proceeds as follows
   * 1. Use Sql Server Master instance to create the data source and external table.
   * Note , per data pool design external tables cannot be created in the 'master' database
   * and external table
   * creation is data base altering and that cannot be done. So the external table
   * must be created in
   * user specified database only.
   * 2. Find the data pool nodes. This uses a utility getDataPoolNodeList
   * 3. Use the returned node hostnames to construct URL and create connections to each of the
   * data nodes. Pass this connection to each of the executors to write their partition
   * of the dataframe.
   */
  def write(
      df: DataFrame,
      colMetaData: Array[ColumnMetadata],
      options: SQLServerBulkJdbcOptions,
      appId: String): Unit = {
    logInfo("write : best effort  write to datapools called")

    val dfColMetadata = colMetaData
    val hostnames = DataPoolUtils.getDataPoolNodeList(options)
    if (hostnames.isEmpty) {
      throw new SQLException(s""" ${hostnames.length} datapool nodes found.
           |DataPools are not configured or non reachable:""".stripMargin)
    }
    logDebug(s"write:${hostnames.length} datapool nodes found : ${hostnames.mkString(" ")}")
    logDebug("write:Will order the Executor action now")
    val dbname = getDBNameFromURL(options.url)
    logDebug("write:user URL " + s"$dbname")
    df.rdd
      .mapPartitionsWithIndex((index, iterator) => {
        options.dataPoolDistPolicy match {
          case "ROUND_ROBIN" =>
            val hostname = hostnames(index % hostnames.length)
            logInfo(
              s"write: partition index $index to host $hostname " +
                s"with distribution policy ROUND_ROBIN")
            saveToDataPoolNode(iterator, hostname, options, dfColMetadata)
          case "REPLICATED" =>
            val host_itr_map = getHostIteratorMap(iterator, hostnames)
            host_itr_map.foreach({
              case (hostname, itr) =>
                logInfo(
                  s"write: partition index $index which hasElem is ${itr.hasNext} " +
                    s"to host $hostname with distribution policy REPLICATED")
                saveToDataPoolNode(itr, hostname, options, dfColMetadata)
            })
          case _ =>
            throw new SQLException(
              s""" Invalid value in dataPoolDistPolicy ${options.dataPoolDistPolicy}  .
                 | Internal feature usage error:""".stripMargin)
        }
        logInfo(s"write:Executor: Saved partition index $index")
        Iterator[Int](0)
      })
      .collect()
  }

  /**
   * saveToDataPoolNode create the right data pool connection url
   * and calls saves parition to save the rows to the data pool node.
   */
  def saveToDataPoolNode(
      iterator: Iterator[Row],
      hostname: String,
      options: SQLServerBulkJdbcOptions,
      dfColMetadata: Array[ColumnMetadata]): Unit = {
    logInfo(s"write: to hostname $hostname")
    val url = DataPoolUtils.createDataPoolURL(hostname, options)
    val newOptions = new SQLServerBulkJdbcOptions(options.parameters + ("url" -> url))
    savePartition(iterator, options.dbtable, dfColMetadata, newOptions)
  }

  /**
   * Used in REPLICATE scenario where a single partition represented by an iterator is written
   * to multiple connection. Scala iterators can only be traversed once. As a result need
   * clone the original iterator per hostname.
   * getHostIteratorMap returns a map of hostnames and associated iterator[Row].
   * This iterator can then be used for writing to the respective host.
   * Refer : https://www.scala-lang.org/api/2.11.2/index.html#scala.collection.Iterator
   */
  def getHostIteratorMap(
      itr: Iterator[Row],
      hostnames: List[String]): Map[String, Iterator[Row]] = {
    val host_itr_map = scala.collection.mutable.Map[String, Iterator[Row]]()
    var itr_orignal = itr
    hostnames.foreach(hostname => {
      val (itr_new, itr_second) = itr_orignal.duplicate
      host_itr_map(hostname) = itr_new
      itr_orignal = itr_second
    })
    host_itr_map.toMap
  }
  def strategy(): Int = {
    SQLServerBulkJdbcOptions.BEST_EFFORT
  }
}
